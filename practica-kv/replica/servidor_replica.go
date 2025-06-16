package main

import (
	"context"
	"log"
	"sync"
	"encoding/binary"
	"fmt"
	"net"
	"os"

	pb "practica-kv/proto" 
	"google.golang.org/grpc"
)

// VectorReloj representa un reloj vectorial de longitud 3 (tres réplicas).
type VectorReloj [3]uint64

// Incrementar aumenta en 1 el componente correspondiente a la réplica que llama.
func (vr *VectorReloj) Incrementar(idReplica int) {
	if idReplica >= 0 && idReplica < 3 {
		vr[idReplica]++
	}
}

// Fusionar toma el máximo elemento a elemento entre dos vectores.
func (vr *VectorReloj) Fusionar(otro VectorReloj) {
	for i := 0; i < 3; i++ {
		if otro[i] > vr[i] {
			vr[i] = otro[i]
		}
	}
}

// AntesDe devuelve true si vr < otro en el sentido estricto
func (vr VectorReloj) AntesDe(otro VectorReloj) bool {
	menor := false
	for i := 0; i < 3; i++ {
		if vr[i] > otro[i] {
			return false
		} else if vr[i] < otro[i] {
			menor = true
		}
	}
	return menor
}

// encodeVector serializa el VectorReloj a []byte
func encodeVector(vr VectorReloj) []byte {
	buf := make([]byte, 8*3)
	for i := 0; i < 3; i++ {
		binary.BigEndian.PutUint64(buf[i*8:(i+1)*8], vr[i])
	}
	return buf
}

// decodeVector convierte []byte a VectorReloj.
func decodeVector(b []byte) VectorReloj {
	var vr VectorReloj
	for i := 0; i < 3; i++ {
		vr[i] = binary.BigEndian.Uint64(b[i*8 : (i+1)*8])
	}
	return vr
}
// ValorConVersion guarda el valor y su reloj vectorial asociado.
type ValorConVersion struct {
	Valor       []byte
	RelojVector VectorReloj
}

// ServidorReplica implementa pb.ReplicaServer
type ServidorReplica struct {
	pb.UnimplementedReplicaServer

	mu           sync.Mutex
	almacen      map[string]ValorConVersion
	relojVector  VectorReloj
	idReplica    int
	clientesPeer []pb.ReplicaClient 
}
// NewServidorReplica crea una instancia de ServidorReplica
// idReplica: 0, 1 o 2
// peerAddrs: direcciones gRPC de los otros dos peers (ej.: []string{":50052", ":50053"})
func NewServidorReplica(idReplica int, peerAddrs []string) *ServidorReplica {
	var clientes []pb.ReplicaClient

	for _, addr := range peerAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Printf("Error al conectar con %s: %v", addr, err)
			continue
		}
		clientes = append(clientes, pb.NewReplicaClient(conn))
	}

	return &ServidorReplica{
		almacen:      make(map[string]ValorConVersion),
		relojVector:  VectorReloj{},
		idReplica:    idReplica,
		clientesPeer: clientes,
	}
}
// GuardarLocal recibe la petición del Coordinador para almacenar clave/valor.
func (r *ServidorReplica) GuardarLocal(ctx context.Context, req *pb.SolicitudGuardar) (*pb.RespuestaGuardar, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	//Incrementar el componente de esta réplica en el reloj vectorial
	r.relojVector.Incrementar(r.idReplica)

	//Guardar el valor con su reloj vectorial
	r.almacen[req.Clave] = ValorConVersion{
		Valor:       req.Valor,
		RelojVector: r.relojVector,
	}
		//log del paso 8
	log.Printf("[Replica %d] GUARDAR clave=%s valor=%s reloj=%v",
		r.idReplica, req.Clave, string(req.Valor), r.relojVector)


	//Construir mutación
	mut := &pb.Mutacion{
		Tipo:        pb.Mutacion_GUARDAR,
		Clave:       req.Clave,
		Valor:       req.Valor,
		RelojVector: encodeVector(r.relojVector),
	}

	// Replicar a los otros peers (en goroutines)
	for _, peer := range r.clientesPeer {
		go func(p pb.ReplicaClient) {
			_, err := p.ReplicarMutacion(context.Background(), mut)
			if err != nil {
				log.Printf("Error replicando a peer: %v", err)
			}
		}(peer)
	}

	//Responder al Coordinador
	return &pb.RespuestaGuardar{
		Exito:           true,
		NuevoRelojVector: encodeVector(r.relojVector),
	}, nil
}
// EliminarLocal recibe la petición del Coordinador para borrar una clave.
func (r *ServidorReplica) EliminarLocal(ctx context.Context, req *pb.SolicitudEliminar) (*pb.RespuestaEliminar, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Incrementar componente local del reloj
	r.relojVector.Incrementar(r.idReplica)

	// Eliminar del mapa si existe
	delete(r.almacen, req.Clave)

	//Crear mutación de eliminación
	mut := &pb.Mutacion{
		Tipo:        pb.Mutacion_ELIMINAR,
		Clave:       req.Clave,
		Valor:       nil,
		RelojVector: encodeVector(r.relojVector),
	}

	// Replicar a los otros peers (en goroutines)
	for _, peer := range r.clientesPeer {
		go func(p pb.ReplicaClient) {
			_, err := p.ReplicarMutacion(context.Background(), mut)
			if err != nil {
				log.Printf("Error replicando eliminación a peer: %v", err)
			}
		}(peer)
	}

	// Responder al Coordinador
	return &pb.RespuestaEliminar{
		Exito:           true,
		NuevoRelojVector: encodeVector(r.relojVector),
	}, nil
}
// ObtenerLocal retorna el valor y reloj vectorial de una clave en esta réplica.
func (r *ServidorReplica) ObtenerLocal(ctx context.Context, req *pb.SolicitudObtener) (*pb.RespuestaObtener, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	val, ok := r.almacen[req.Clave]
	if !ok {
		return &pb.RespuestaObtener{
			Existe: false,
		}, nil
	}

	return &pb.RespuestaObtener{
		Valor:       val.Valor,
		RelojVector: encodeVector(val.RelojVector),
		Existe:      true,
	}, nil
}
// ReplicarMutacion recibe una mutación de otra réplica y la aplica localmente.
func (r *ServidorReplica) ReplicarMutacion(ctx context.Context, m *pb.Mutacion) (*pb.Reconocimiento, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	vectorRemoto := decodeVector(m.RelojVector)
	//log del paso 8
	log.Printf("[Replica %d] REPLICACIÓN recibida: tipo=%v clave=%s valor=%s reloj=%v",
		r.idReplica, m.Tipo, m.Clave, string(m.Valor), vectorRemoto)

	valActual, existe := r.almacen[m.Clave]

	if !existe || valActual.RelojVector.AntesDe(vectorRemoto) {
		if m.Tipo == pb.Mutacion_GUARDAR {
			r.almacen[m.Clave] = ValorConVersion{
				Valor:       m.Valor,
				RelojVector: vectorRemoto,
			}
		} else if m.Tipo == pb.Mutacion_ELIMINAR {
			delete(r.almacen, m.Clave)
		}
		// Fusionar relojes
		r.relojVector.Fusionar(vectorRemoto)
	} else {
		log.Printf("Conflicto ignorado: versión local es más reciente para clave %s", m.Clave)
	}

	return &pb.Reconocimiento{
		Ok:             true,
		RelojVectorAck: encodeVector(r.relojVector),
	}, nil
}
func main() {
	if len(os.Args) != 5 {
		log.Fatalf("Uso: %s <idReplica> <direccionEscucha> <peer1> <peer2>", os.Args[0])
	}

	idReplica := os.Args[1]
	direccion := os.Args[2]
	peer1 := os.Args[3]
	peer2 := os.Args[4]

	// Convertir idReplica a int
	var id int
	_, err := fmt.Sscanf(idReplica, "%d", &id)
	if err != nil || id < 0 || id > 2 {
		log.Fatalf("ID de réplica inválido: %s", idReplica)
	}

	// Inicializar gRPC
	lis, err := net.Listen("tcp", direccion)
	if err != nil {
		log.Fatalf("Error al escuchar en %s: %v", direccion, err)
	}
	s := grpc.NewServer()

	replica := NewServidorReplica(id, []string{peer1, peer2})
	pb.RegisterReplicaServer(s, replica)

	log.Printf("Réplica %d escuchando en %s", id, direccion)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Error al servir: %v", err)
	}
}
