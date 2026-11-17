/* Gerador de logs sinteticos */ 

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

static uint32_t state = 123456789u;
static uint32_t rnd(void) {
  state ^= state << 13; state ^= state >> 17; state ^= state << 5;
  return state;
}
static int number(const char * s, long * v) {
  char * end;
  errno = 0;
  long n = strtol(s, & end, 10);
  if (errno || * s == '\0' || * end || n < 1) return 0;
  * v = n;
  return 1;
}

int main(int argc, char ** argv) {
  if (argc < 4 || argc > 6) {
    fprintf(stderr, "Uso: %s <arquivos> <linhas_por_arquivo> <palavras_por_linha> [seed] [diretorio]\n", argv[0]);
    return 1;
  }

  long files, lines, words, seed = 12345;
  if (!number(argv[1], & files) || !number(argv[2], & lines) || !number(argv[3], & words) || files > 100000 || lines > 10000000 || words > 10000) {
    fprintf(stderr, "Parametros invalidos.\n");
    return 1;
  }
  if (argc >= 5 && !number(argv[4], & seed)) {
    fprintf(stderr, "Seed invalida.\n");
    return 1;
  }

  state = (uint32_t) seed;
  const char * dir = argc == 6 ? argv[5] : "logs";
  if (mkdir(dir, 0775) != 0 && errno != EEXIST) {
    perror("mkdir");
    return 1;
  }

  const char * terms[] = {
    "usuario",
    "pedido",
    "servico",
    "conexao",
    "banco",
    "requisicao",
    "tempo",
    "memoria",
    "rede",
    "processo",
    "cache",
    "resposta",
    "operacao",
    "cliente",
    "servidor"
  };
  const char * levels[] = {
    "INFO",
    "INFO",
    "INFO",
    "INFO",
    "WARNING",
    "ERROR",
    "DEBUG"
  };
  for (long i = 0; i < files; i++) {
    char path[1024];
    int n = snprintf(path, sizeof(path), "%s/log_%04ld.log", dir, i + 1);
    if (n < 0 || (size_t) n >= sizeof(path)) {
      fprintf(stderr, "Caminho muito longo.\n");
      return 1;
    }

    FILE * f = fopen(path, "w");
    if (!f) {
      perror(path);
      return 1;
    }

    for (long j = 0; j < lines; j++) {
      const char * level = levels[rnd() % 7];
      fprintf(f, "2026-10-%02uT%02u:%02u:%02u [%s] ", 1 + (unsigned)(rnd() % 28), (unsigned)(rnd() % 24), (unsigned)(rnd() % 60), (unsigned)(rnd() % 60), level);
      for (long k = 0; k < words; k++) fprintf(f, "%s%s", terms[rnd() % 15], k + 1 == words ? "" : " ");
      fputc('\n', f);
    }
    if (fclose(f) != 0) {
      perror("fclose");
      return 1;
    }
  }
  printf("Gerados %ld arquivos em %s/ (%ld linhas/arquivo, %ld palavras de mensagem/linha; seed=%ld)\n", files, dir, lines, words, seed);
  return 0;
}