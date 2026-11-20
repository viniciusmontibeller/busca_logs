#define _POSIX_C_SOURCE 200809L
#include <mpi.h>
#include <dirent.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#define MAX_CAMINHO 4096
#define TAG_TAREFA 1
#define TAG_CAMINHO 2
#define TAG_RESULTADO 3
#define TAG_TEXTO 4
#define TAG_QUANTIDADE 5

typedef struct {
  char *nome_arquivo;
  char *caminho;
  char *resultado;
  int ocorrencias;
} Busca;

int comparar_buscas(const void *a, const void *b) {
  const Busca *busca_a = a;
  const Busca *busca_b = b;

  return strcmp(busca_a->nome_arquivo, busca_b->nome_arquivo);
}

// todos os processos precisam acessar os arquivos
Busca *listar_arquivos(const char *diretorio, int *total) {
  DIR *pasta = opendir(diretorio);
  if (pasta == NULL) {
    printf("nao foi possivel abrir o diretorio");
    MPI_Abort(MPI_COMM_WORLD, 1);
  };

  Busca *buscas = NULL;
  *total = 0;
  struct dirent *entrada;

  while ((entrada = readdir(pasta)) != NULL) {
    const char *nome = entrada->d_name;
    size_t tamanho_nome = strlen(nome);

    if (tamanho_nome < 4 || strcmp(nome + tamanho_nome - 4, ".log") != 0) {
      continue;
    };

    size_t tamanho_caminho = strlen(diretorio) + tamanho_nome + 2;
    if (tamanho_caminho > MAX_CAMINHO) {
      printf("caminho de arquivo muito longo");
      MPI_Abort(MPI_COMM_WORLD, 1);
    }

    char caminho[MAX_CAMINHO];
    snprintf(caminho, sizeof(caminho), "%s/%s", diretorio, nome);

    struct stat info;
    if (stat(caminho, &info) != 0 || !S_ISREG(info.st_mode)) {
      continue;
    }

    Busca *novo = realloc(buscas, (size_t)(*total + 1) * sizeof(Busca));
    if (novo == NULL) {
      printf("falha ao alocar lista de arquivos");
      MPI_Abort(MPI_COMM_WORLD, 1);
    };

    buscas = novo;

    buscas[*total].nome_arquivo = strdup(nome);
    buscas[*total].caminho = strdup(caminho);
    buscas[*total].resultado = NULL;
    buscas[*total].ocorrencias = 0;

    if (buscas[*total].nome_arquivo == NULL || buscas[*total].caminho == NULL) {
      printf("falha ao copiar nome de arquivo");
      MPI_Abort(MPI_COMM_WORLD, 1);
    }

    (*total)++;
  }

  closedir(pasta);
  qsort(buscas, (size_t)*total, sizeof(Busca), comparar_buscas);
  return buscas;
}

void adicionar_linha(char **resultado, size_t *tamanho, size_t *capacidade, const char *nome, int numero_linha, const char *linha) {
  int prefixo = snprintf(NULL, 0, "%s:%d: ", nome, numero_linha);
  if (prefixo < 0) {
    printf("falha ao formatar resultado");
    MPI_Abort(MPI_COMM_WORLD, 1);
  };

  size_t tamanho_linha = strlen(linha);
  size_t adicional = (size_t)prefixo + tamanho_linha + (tamanho_linha == 0 || linha[tamanho_linha - 1] != '\n');

  if (*tamanho > (size_t)INT_MAX || adicional > (size_t)INT_MAX - *tamanho){
    printf("resultado de um arquivo excede o tamanho de uma mensagem MPI");
    MPI_Abort(MPI_COMM_WORLD, 1);
  };

  size_t necessario = *tamanho + adicional + 1;
  if (necessario > *capacidade) {
    size_t nova_capacidade = *capacidade ? *capacidade : 256;
    while (nova_capacidade < necessario) nova_capacidade *= 2;
    char *novo = realloc(*resultado, nova_capacidade);
    if (novo == NULL) { 
      printf("falha ao guardar resultados");
      MPI_Abort(MPI_COMM_WORLD, 1);
    };
    *resultado = novo;
    *capacidade = nova_capacidade;
  };

  int escritos = snprintf(*resultado + *tamanho, *capacidade - *tamanho, "%s:%d: ", nome, numero_linha);
  *tamanho += (size_t)escritos;
  memcpy(*resultado + *tamanho, linha, tamanho_linha);
  *tamanho += tamanho_linha;

  if (tamanho_linha == 0 || linha[tamanho_linha - 1] != '\n') {
    (*resultado)[(*tamanho)++] = '\n';
  };

  (*resultado)[*tamanho] = '\0';
};

void processar_busca_em_arquivo(Busca *busca, const char *termo) {
  FILE *arquivo = fopen(busca->caminho, "r");

  if (arquivo == NULL) {
      fprintf(stderr, "Nao foi possivel abrir: %s\n", busca->caminho);
      MPI_Abort(MPI_COMM_WORLD, 1);
  }

  char *linha = NULL;
  size_t capacidade_linha = 0;
  size_t tamanho = 0, capacidade = 0;
  int numero_linha = 0;

  busca->resultado = NULL;
  busca->ocorrencias = 0;

  while (getline(&linha, &capacidade_linha, arquivo) != -1) {
    numero_linha++;

    if (strstr(linha, termo) != NULL) {
      busca->ocorrencias++;

      adicionar_linha(
        &busca->resultado,
        &tamanho,
        &capacidade,
        busca->nome_arquivo,
        numero_linha,
        linha
      );
    }
  }

  if (ferror(arquivo)) {
    fprintf(stderr, "Falha na leitura: %s\n", busca->caminho);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  free(linha);
  fclose(arquivo);
};

void enviar_resultados_static(Busca *buscas, int *tarefas, int quantidade) {
  int tamanho_buffer = 0;
  int tamanho_int, tamanho_char;

  MPI_Pack_size(1, MPI_INT, MPI_COMM_WORLD, &tamanho_int);
  tamanho_buffer += tamanho_int;

  for (int i = 0; i < quantidade; i++) {
    int indice = tarefas[i];

    int tamanho_texto = buscas[indice].resultado
      ? (int)strlen(buscas[indice].resultado)
      : 0;

    MPI_Pack_size(3, MPI_INT, MPI_COMM_WORLD, &tamanho_int);
    tamanho_buffer += tamanho_int;

    MPI_Pack_size(tamanho_texto, MPI_CHAR, MPI_COMM_WORLD, &tamanho_char);
    tamanho_buffer += tamanho_char;
  }

  char *buffer = malloc(tamanho_buffer);

  if (buffer == NULL){
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  int posicao = 0;

  MPI_Pack(&quantidade, 1, MPI_INT, buffer, tamanho_buffer, &posicao, MPI_COMM_WORLD);

  for (int i = 0; i < quantidade; i++) {
    int indice = tarefas[i];
    int ocorrencias = buscas[indice].ocorrencias;

    int tamanho_texto = buscas[indice].resultado
      ? (int)strlen(buscas[indice].resultado)
      : 0;

    MPI_Pack(&indice, 1, MPI_INT, buffer, tamanho_buffer, &posicao, MPI_COMM_WORLD);
    MPI_Pack(&ocorrencias, 1, MPI_INT, buffer, tamanho_buffer, &posicao, MPI_COMM_WORLD);
    MPI_Pack(&tamanho_texto, 1, MPI_INT, buffer, tamanho_buffer, &posicao, MPI_COMM_WORLD);

    if (tamanho_texto > 0) {
      MPI_Pack(buscas[indice].resultado, tamanho_texto, MPI_CHAR, buffer, tamanho_buffer, &posicao, MPI_COMM_WORLD);
    }
  }

  MPI_Send(buffer, posicao, MPI_PACKED, 0, TAG_RESULTADO, MPI_COMM_WORLD);

  free(buffer);
};

void receber_resultados_static(Busca *buscas, int total) {
  MPI_Status status;
  int tamanho_buffer;

  MPI_Probe(MPI_ANY_SOURCE, TAG_RESULTADO, MPI_COMM_WORLD, &status);
  MPI_Get_count(&status, MPI_PACKED, &tamanho_buffer);

  if (tamanho_buffer <= 0){
    MPI_Abort(MPI_COMM_WORLD, 1);
  };

  char *buffer = malloc(tamanho_buffer);

  if (buffer == NULL){
    MPI_Abort(MPI_COMM_WORLD, 1);
  };

  MPI_Recv(buffer, tamanho_buffer, MPI_PACKED, status.MPI_SOURCE, TAG_RESULTADO, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  int posicao = 0;
  int quantidade;

  MPI_Unpack(buffer, tamanho_buffer, &posicao, &quantidade, 1, MPI_INT, MPI_COMM_WORLD);

  if (quantidade < 0 || quantidade > total){
    MPI_Abort(MPI_COMM_WORLD, 1);
  };

  for (int i = 0; i < quantidade; i++) {
    int indice, ocorrencias, tamanho_texto;

    MPI_Unpack(buffer, tamanho_buffer, &posicao, &indice, 1, MPI_INT, MPI_COMM_WORLD);
    MPI_Unpack(buffer, tamanho_buffer, &posicao, &ocorrencias, 1, MPI_INT, MPI_COMM_WORLD);
    MPI_Unpack(buffer, tamanho_buffer, &posicao, &tamanho_texto, 1, MPI_INT, MPI_COMM_WORLD);

    if (indice < 0 || indice >= total || ocorrencias < 0 || tamanho_texto < 0) {
      MPI_Abort(MPI_COMM_WORLD, 1);
    }

    char *texto = malloc((size_t)tamanho_texto + 1);

    if (texto == NULL){
      MPI_Abort(MPI_COMM_WORLD, 1);
    };

    if (tamanho_texto > 0) {
      MPI_Unpack(buffer, tamanho_buffer, &posicao, texto, tamanho_texto, MPI_CHAR, MPI_COMM_WORLD);
    };

    texto[tamanho_texto] = '\0';

    buscas[indice].ocorrencias = ocorrencias;

    free(buscas[indice].resultado);
    buscas[indice].resultado = texto;
  }

  free(buffer);
};

void enviar_resultado_dynamic(int indice, Busca *busca) {
  int tamanho_texto = busca->resultado ? (int)strlen(busca->resultado) : 0;

  int cabecalho[2] = {indice, busca->ocorrencias};
  int tamanho_total = sizeof(cabecalho) + tamanho_texto;
  char *mensagem = malloc(tamanho_total);

  if (mensagem == NULL){
    MPI_Abort(MPI_COMM_WORLD, 1);
  };

  memcpy(mensagem, cabecalho, sizeof(cabecalho));

  if (tamanho_texto > 0) {
    memcpy(
      mensagem + sizeof(cabecalho),
      busca->resultado,
      tamanho_texto
    );
  }

  MPI_Send(mensagem, tamanho_total, MPI_BYTE, 0, TAG_RESULTADO, MPI_COMM_WORLD);

  free(mensagem);
}

int receber_resultado_dynamic(Busca *buscas, int total) {
  MPI_Status status;
  int tamanho;

  MPI_Probe(MPI_ANY_SOURCE, TAG_RESULTADO, MPI_COMM_WORLD, &status);
  MPI_Get_count(&status, MPI_BYTE, &tamanho);

  if (tamanho < (int)(2 * sizeof(int))) {
    MPI_Abort(MPI_COMM_WORLD, 1);
  };

  char *mensagem = malloc(tamanho);

  if (mensagem == NULL) {
    MPI_Abort(MPI_COMM_WORLD, 1);
  };

  MPI_Recv(mensagem, tamanho, MPI_BYTE, status.MPI_SOURCE, TAG_RESULTADO, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  int cabecalho[2];
  memcpy(cabecalho, mensagem, sizeof(cabecalho));

  int indice = cabecalho[0];
  int ocorrencias = cabecalho[1];

  if (indice < 0 || indice >= total) {
    MPI_Abort(MPI_COMM_WORLD, 1);
  };

  int tamanho_texto = tamanho - sizeof(cabecalho);

  char *texto = malloc((size_t)tamanho_texto + 1);

  if (texto == NULL) {
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  memcpy(texto, mensagem + sizeof(cabecalho), tamanho_texto);

  texto[tamanho_texto] = '\0';

  buscas[indice].ocorrencias = ocorrencias;
  free(buscas[indice].resultado);
  buscas[indice].resultado = texto;

  free(mensagem);

  return status.MPI_SOURCE;
}

void master_static(Busca *buscas, int total, int processos) {
  int workers = processos - 1;

  for (int rank = 1; rank < processos; rank++) {
    int *tarefas = malloc((total > 0 ? total : 1) * sizeof(int));

    if (tarefas == NULL) {
      printf("falha ao alocar tarefas");
      MPI_Abort(MPI_COMM_WORLD, 1);
    };

    int quantidade = 0;

    // distribuicao ciclica
    for (int i = rank - 1; i < total; i += workers) {
      tarefas[quantidade] = i;
      quantidade++;
    };

    MPI_Send(tarefas, quantidade, MPI_INT, rank, 0, MPI_COMM_WORLD);
    free(tarefas);
  };

  for (int rank = 1; rank < processos; rank++) {
    receber_resultados_static(buscas, total);
  };
};

void worker_static(Busca *buscas, const char *termo) {
  MPI_Status status;
  int quantidade;

  //verifica na mensagem o tamanho do trabalho enviado para alocar espaco
  MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
  MPI_Get_count(&status, MPI_INT, &quantidade);

  int *tarefas = malloc((quantidade > 0 ? quantidade : 1) * sizeof(int));

  if (tarefas == NULL) {
    printf("Erro ao alocar memória");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  MPI_Recv(tarefas, quantidade, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  for (int i = 0; i < quantidade; i++) {
    int indice_arquivo = tarefas[i];

    processar_busca_em_arquivo(&buscas[indice_arquivo], termo);
  };

  enviar_resultados_static(buscas, tarefas, quantidade);

  for (int i = 0; i < quantidade; i++) {
    int indice = tarefas[i];

    free(buscas[indice].resultado);
    buscas[indice].resultado = NULL;
  }

  free(tarefas);
};

void master_dynamic(Busca *buscas, int total, int processos) {
   int proximo = 0;

  for (int rank = 1; rank < processos; rank++) {
    int tarefa = proximo < total ? proximo++ : -1;

    MPI_Send(&tarefa, 1, MPI_INT, rank, TAG_TAREFA, MPI_COMM_WORLD);
  }

  // recebe resultado e envia proxima tarefa se tiver
  for (int concluidos = 0; concluidos < total; concluidos++) {
    int rank = receber_resultado_dynamic(buscas, total);

    int tarefa = proximo < total ? proximo++ : -1;

    MPI_Send(&tarefa, 1, MPI_INT, rank, TAG_TAREFA, MPI_COMM_WORLD);
  }
};

void worker_dynamic(Busca *buscas, const char *termo) {
  while (1) {
    int indice;

    MPI_Recv(&indice, 1, MPI_INT, 0,TAG_TAREFA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    if (indice == -1)
      break;

    processar_busca_em_arquivo(&buscas[indice], termo);

    enviar_resultado_dynamic(indice, &buscas[indice]);

    free(buscas[indice].resultado);
    buscas[indice].resultado = NULL;
  }
};

void imprimir_resultados(Busca *buscas, int total, double tempo) {
  long total_ocorrencias = 0;

  printf("=== RESULTADOS DA BUSCA ===\n");
  for (int i = 0; i < total; i++) {
    if (buscas[i].resultado) {
      printf("%s", buscas[i].resultado);
    }
    total_ocorrencias += buscas[i].ocorrencias;
  }

  printf("\n=== RESUMO ===\nTotal de arquivos: %d\nTotal de ocorrencias: %ld\n",total, total_ocorrencias);
  for (int i = 0; i < total; i++) {
    printf("%s: %d ocorrencias\n", buscas[i].nome_arquivo, buscas[i].ocorrencias);
  }

  printf("\nTempo de execucao: %.6f segundos\n", tempo);
}

int main(int argc, char *argv[]) {
  MPI_Init(&argc, &argv);
  int rank, processos;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &processos);

  const char *diretorio = NULL, *termo = NULL, *modo = NULL;

  for (int i = 1; i + 1 < argc; i += 2) {
    if (strcmp(argv[i], "--dir") == 0) {
      diretorio = argv[i+1];
    }
    else if (strcmp(argv[i], "--search") == 0) {
      termo = argv[i + 1];
    }
    else if (strcmp(argv[i], "--mode") == 0) {
      modo = argv[i + 1];
    }
  }

  if (processos < 2 || !diretorio || !termo || termo[0] == '\0' || !modo || (strcmp(modo, "static") != 0 && strcmp(modo, "dynamic") != 0)) {
    if (rank == 0){
      fprintf(stderr, "Uso: mpirun -np N ./busca_logs --dir logs --search ERROR --mode static|dynamic (N >= 2)\n");
    };

    MPI_Finalize();
    return 1;
  }

  Busca *buscas = NULL;
  int total = 0;
  buscas = listar_arquivos(diretorio, &total);
  // barreira para esperar todos os processos para iniciar o tempo
  MPI_Barrier(MPI_COMM_WORLD);
  
  if (rank == 0) {
    double inicio = MPI_Wtime();
    if (strcmp(modo, "static") == 0) {
      master_static(buscas, total, processos);
    }
    else {
      master_dynamic(buscas, total, processos);
    }

    double tempo = MPI_Wtime() - inicio;

    imprimir_resultados(buscas, total, tempo);
  }
  else {
    if (strcmp(modo, "static") == 0) {
      worker_static(buscas, termo);
    }
    else {
      worker_dynamic(buscas, termo);
    }
  }

  for (int i = 0; i < total; i++) {
    free(buscas[i].nome_arquivo);
    free(buscas[i].caminho);
    free(buscas[i].resultado);
  }

  free(buscas);

  MPI_Finalize();
  return 0;
  }