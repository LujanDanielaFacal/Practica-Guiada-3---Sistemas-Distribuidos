package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "practica-kv/proto"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial(":6000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("No se pudo conectar con el Coordinador: %v", err)
	}
	defer conn.Close()

	client := pb.NewCoordinadorClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	clave := "usuario123"
	valor := []byte("datosImportantes")

	// 1. Guardar
	fmt.Println("Guardando...")
	resGuardar, err := client.Guardar(ctx, &pb.SolicitudGuardar{
		Clave:       clave,
		Valor:       valor,
		RelojVector: nil,
	})
	if err != nil {
		log.Fatalf("Error al guardar: %v", err)
	}
	fmt.Printf("Guardado exitosamente. Reloj: %v\n", resGuardar.NuevoRelojVector)

	// 2. Obtener
	fmt.Println("Obteniendo...")
	resObtener, err := client.Obtener(ctx, &pb.SolicitudObtener{Clave: clave})
	if err != nil {
		log.Fatalf("Error al obtener: %v", err)
	}
	if resObtener.Existe {
		fmt.Printf("Valor: %s\n", string(resObtener.Valor))
		fmt.Printf("Reloj: %v\n", resObtener.RelojVector)
	} else {
		fmt.Println("Clave no encontrada.")
	}

	// 3. Eliminar
	fmt.Println("Eliminando...")
	resEliminar, err := client.Eliminar(ctx, &pb.SolicitudEliminar{
		Clave:       clave,
		RelojVector: resObtener.RelojVector, // Enviar el vector recibido
	})
	if err != nil {
		log.Fatalf("Error al eliminar: %v", err)
	}
	fmt.Printf("Eliminado exitosamente. Reloj: %v\n", resEliminar.NuevoRelojVector)

	// 4. Obtener nuevamente
	fmt.Println("Obteniendo nuevamente...")
	resObtener2, err := client.Obtener(ctx, &pb.SolicitudObtener{Clave: clave})
	if err != nil {
		log.Fatalf("Error al obtener nuevamente: %v", err)
	}
	if resObtener2.Existe {
		fmt.Printf("Valor: %s\n", string(resObtener2.Valor))
	} else {
		fmt.Println("Clave eliminada correctamente.")
	}
}
