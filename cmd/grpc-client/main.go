// Cliente gRPC de demonstração acadêmica para o microserviço Botão de Pânico (CIT7219).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/plataforma-mulher/botao-panico/gen/alerta/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func main() {
	endereco := os.Getenv("GRPC_ADDR")
	if endereco == "" {
		endereco = "127.0.0.1:50051"
	}

	chave := os.Getenv("SERVICE_KEY")

	fmt.Printf("=== Conectando ao Botão de Pânico gRPC em %s ===\n", endereco)

	conn, err := grpc.NewClient(
		endereco,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Falha ao discar para servidor gRPC: %v", err)
	}
	defer conn.Close()

	client := alertav1.NewAlertasServiceClient(conn)

	// Injeta metadados de autenticação se SERVICE_KEY estiver configurada
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if chave != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-service-token", chave)
		fmt.Println("✓ Autenticação serviço-a-serviço injetada via metadados")
	}

	// 1. Criar Alerta
	fmt.Println("\n1. Enviando solicitação CriarAlerta (Protocol Buffers binário)...")
	chaveIdemp := fmt.Sprintf("demo-grpc-%d", time.Now().UnixNano())
	reqCriar := &alertav1.CriarAlertaRequest{
		UsuariaId: "usuaria-academica-demo",
		Nivel:     alertav1.NivelRisco_ALTO,
		Mensagem:  "Demonstração CIT7219 - Acionamento de emergência gRPC",
		Localizacao: &alertav1.Coordenadas{
			Latitude:  -28.9346,
			Longitude: -49.4957,
		},
		Silencioso:        false,
		ChaveIdempotencia: chaveIdemp,
	}

	resCriar, err := client.CriarAlerta(ctx, reqCriar)
	if err != nil {
		log.Fatalf("Erro ao criar alerta via gRPC: %v", err)
	}

	alertaID := resCriar.Alerta.Id
	fmt.Printf("✓ Alerta criado com sucesso!\n")
	fmt.Printf("  ID: %s\n", alertaID)
	fmt.Printf("  Status: %s\n", resCriar.Alerta.Status)
	fmt.Printf("  Endereço Aproximado: %s\n", resCriar.Alerta.EnderecoAproximado)
	fmt.Printf("  Notificações simuladas: %d\n", len(resCriar.Alerta.Notificacoes))

	// 2. Buscar Alerta
	fmt.Println("\n2. Consultando alerta recém-criado via BuscarAlerta...")
	resBuscar, err := client.BuscarAlerta(ctx, &alertav1.BuscarAlertaRequest{Id: alertaID})
	if err != nil {
		log.Fatalf("Erro ao buscar alerta via gRPC: %v", err)
	}
	fmt.Printf("✓ Alerta localizado: ID %s, Usuária: %s, Criado em: %s\n",
		resBuscar.Alerta.Id, resBuscar.Alerta.UsuariaId, resBuscar.Alerta.CriadoEm.AsTime().Format(time.RFC3339))

	// 3. Teste de Idempotência
	fmt.Println("\n3. Reenviando requisição com a mesma chave de idempotência...")
	resReplay, err := client.CriarAlerta(ctx, reqCriar)
	if err != nil {
		log.Fatalf("Erro na retentativa gRPC: %v", err)
	}
	fmt.Printf("✓ Resposta idempotente confirmada! Replay: %t, ID recuperado: %s\n",
		resReplay.Replay, resReplay.Alerta.Id)

	// 4. Listar Instituições de Proteção
	fmt.Println("\n4. Consultando rede de instituições de Araranguá...")
	resInst, err := client.ListarInstituicoes(ctx, &alertav1.ListarInstituicoesRequest{})
	if err != nil {
		log.Fatalf("Erro ao listar instituições via gRPC: %v", err)
	}
	fmt.Printf("✓ %d instituições encontradas:\n", len(resInst.Instituicoes))
	for _, inst := range resInst.Instituicoes {
		fmt.Printf("  - %s (%s): %s\n", inst.Nome, inst.Tipo, inst.Telefone)
	}

	fmt.Println("\n=== Demonstração gRPC concluída com sucesso! ===")
}
