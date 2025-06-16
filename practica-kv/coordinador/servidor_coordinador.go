package main

import (
	"context"
	"flag"
	"log"
	"net"
	"sync/atomic"

	pb "practica-kv/proto"
	"google.golang.org/grpc"
)
// ServidorCoordinador implementa pb.CoordinadorServer.
type ServidorCoordinador struct {
	pb.UnimplementedCoordinadorServer

	listaReplicas []string
	indiceRR      uint64 // Round-robin
}

// NewServidorCoordinador crea un Coordinador con direcciones de réplica.
func NewServidorCoordinador(replicas []string) *ServidorCoordinador {
	return &ServidorCoordinador{
		listaReplicas: replicas,
	}
}

// elegirReplicaParaEscritura: round-robin simple (ignora la clave).
func (c *ServidorCoordinador) elegirReplicaParaEscritura(clave string) string {
	idx := atomic.AddUint64(&c.indiceRR, 1)
	return c.listaReplicas[int(idx)%len(c.listaReplicas)]
}

// elegirReplicaParaLectura: también round-robin.
func (c *ServidorCoordinador) elegirReplicaParaLectura() string {
	idx := atomic.AddUint64(&c.indiceRR, 1)
	return c.listaReplicas[int(idx)%len(c.listaReplicas)]
}
// Obtener redirige petición de lectura a una réplica.
func (c *ServidorCoordinador) Obtener(ctx context.Context, req *pb.SolicitudObtener) (*pb.RespuestaObtener, error) {
	addr := c.elegirReplicaParaLectura()
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewReplicaClient(conn)
	return client.ObtenerLocal(ctx, req)
}

// Guardar redirige petición de escritura a una réplica.
func (c *ServidorCoordinador) Guardar(ctx context.Context, req *pb.SolicitudGuardar) (*pb.RespuestaGuardar, error) {
	addr := c.elegirReplicaParaEscritura(req.Clave)
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewReplicaClient(conn)
	return client.GuardarLocal(ctx, req)
}

// Eliminar redirige petición de eliminación a una réplica.
func (c *ServidorCoordinador) Eliminar(ctx context.Context, req *pb.SolicitudEliminar) (*pb.RespuestaEliminar, error) {
	addr := c.elegirReplicaParaEscritura(req.Clave)
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewReplicaClient(conn)
	return client.EliminarLocal(ctx, req)
}
func main() {
	listen := flag.String("listen", ":6000", "Dirección del Coordinador")
	flag.Parse()
	replicas := flag.Args()

	if len(replicas) < 3 {
		log.Fatalf("Debe proveer al menos 3 direcciones de réplicas. Ej: go run servidor_coordinador.go -listen :6000 :50051 :50052 :50053")
	}

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("Fallo al escuchar en %s: %v", *listen, err)
	}

	grpcServer := grpc.NewServer()
	coord := NewServidorCoordinador(replicas)
	pb.RegisterCoordinadorServer(grpcServer, coord)

	log.Printf("Coordinador escuchando en %s", *listen)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Fallo al servir: %v", err)
	}
}
