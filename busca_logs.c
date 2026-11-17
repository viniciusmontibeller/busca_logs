#define _POSIX_C_SOURCE 200809L
#include <mpi.h>
#include <dirent.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* T2: cada arquivo .log e uma tarefa. O diretorio deve ser compartilhado
 * entre os processos MPI (por exemplo, um filesystem comum no cluster). */
enum { TAG_READY = 10, TAG_TASK = 11, TAG_STOP = 12,
       TAG_RESULT = 13, TAG_DATA = 14 };
enum { CHUNK = 65536 };

typedef struct { char **names; int count; } FileList;
typedef struct { uint64_t matches; FILE *data; int error; } Result;

static void die(const char *message) {
    fprintf(stderr, "Erro: %s\n", message);
    MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    exit(EXIT_FAILURE);
}
static void *xmalloc(size_t n) {
    void *p = malloc(n ? n : 1);
    if (!p) die("memoria insuficiente");
    return p;
}
static char *xstrdup(const char *s) {
    char *p = strdup(s);
    if (!p) die("memoria insuficiente");
    return p;
}
static int cmp_strings(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}
static int has_log_suffix(const char *name) {
    size_t n = strlen(name);
    return n >= 4 && strcmp(name + n - 4, ".log") == 0;
}
static char *join_path(const char *dir, const char *file) {
    size_t a = strlen(dir), b = strlen(file);
    if (a > SIZE_MAX - b - 2) die("caminho longo demais");
    char *path = xmalloc(a + b + 2);
    snprintf(path, a + b + 2, "%s/%s", dir, file);
    return path;
}
static FileList find_logs(const char *dir) {
    DIR *d = opendir(dir);
    if (!d) { fprintf(stderr, "Nao foi possivel abrir %s: %s\n", dir, strerror(errno)); die("diretorio invalido"); }
    FileList list = {0};
    struct dirent *entry;
    int cap = 0;
    while ((entry = readdir(d)) != NULL) {
        if (!has_log_suffix(entry->d_name)) continue;
        char *path = join_path(dir, entry->d_name);
        struct stat st;
        int is_regular = (stat(path, &st) == 0 && S_ISREG(st.st_mode));
        free(path);
        if (!is_regular) continue;
        if (list.count == INT_MAX) die("arquivos demais");
        if (list.count == cap) {
            int new_cap = cap ? cap * 2 : 32;
            if (new_cap < cap || new_cap <= list.count) die("arquivos demais");
            char **new_names = realloc(list.names, (size_t)new_cap * sizeof(*new_names));
            if (!new_names) die("memoria insuficiente");
            list.names = new_names;
            cap = new_cap;
        }
        list.names[list.count++] = xstrdup(entry->d_name);
    }
    closedir(d);
    qsort(list.names, (size_t)list.count, sizeof(*list.names), cmp_strings);
    return list;
}
static FileList share_list(FileList list, int rank) {
    /* Um unico broadcast de todos os nomes, separados por '\0'. */
    int total = 0;
    if (rank == 0) {
        for (int i = 0; i < list.count; ++i) {
            size_t n = strlen(list.names[i]) + 1;
            if (n > (size_t)(INT_MAX - total)) die("lista de arquivos excede limite MPI");
            total += (int)n;
        }
    }
    MPI_Bcast(&list.count, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&total, 1, MPI_INT, 0, MPI_COMM_WORLD);
    char *buf = xmalloc((size_t)total);
    if (rank == 0) {
        int pos = 0;
        for (int i = 0; i < list.count; ++i) {
            size_t n = strlen(list.names[i]) + 1;
            memcpy(buf + pos, list.names[i], n);
            pos += (int)n;
        }
    }
    MPI_Bcast(buf, total, MPI_CHAR, 0, MPI_COMM_WORLD);
    if (rank != 0) {
        list.names = xmalloc((size_t)list.count * sizeof(*list.names));
        int pos = 0;
        for (int i = 0; i < list.count; ++i) {
            size_t n = strlen(buf + pos) + 1;
            list.names[i] = xstrdup(buf + pos);
            pos += (int)n;
        }
    }
    free(buf);
    return list;
}
static void free_list(FileList *list) {
    for (int i = 0; i < list->count; ++i) free(list->names[i]);
    free(list->names);
}

static void search_file(const char *dir, const char *name, const char *term,
                        FILE **output, uint64_t *count, int *error) {
    *output = tmpfile();
    if (!*output) die("nao foi possivel criar arquivo temporario");
    *count = 0;
    *error = 0;
    char *path = join_path(dir, name);
    FILE *in = fopen(path, "r");
    if (!in) {
        fprintf(stderr, "Nao foi possivel ler %s: %s\n", path, strerror(errno));
        *error = 1;
        free(path);
        return;
    }
    free(path);
    char *line = NULL;
    size_t capacity = 0;
    ssize_t n;
    uint64_t line_number = 0;
    while ((n = getline(&line, &capacity, in)) != -1) {
        ++line_number;
        if (strstr(line, term) != NULL) {
            if (fprintf(*output, "%s:%" PRIu64 ": ", name, line_number) < 0 ||
                fwrite(line, 1, (size_t)n, *output) != (size_t)n ||
                (n > 0 && line[n - 1] != '\n' && fputc('\n', *output) == EOF)) {
                die("falha na escrita de resultado temporario");
            }
            ++*count;
        }
    }
    if (ferror(in)) {
        fprintf(stderr, "Falha durante leitura de %s\n", name);
        *error = 1;
    }
    free(line);
    fclose(in);
    if (fflush(*output) != 0) die("falha ao gravar resultados");
}
static void send_result(int file_id, const char *dir, const char *name, const char *term) {
    FILE *out;
    uint64_t count;
    int error;
    search_file(dir, name, term, &out, &count, &error);
    if (fseeko(out, 0, SEEK_END) != 0) die("erro ao medir dados");
    off_t size = ftello(out);
    if (size < 0 || fseeko(out, 0, SEEK_SET) != 0) die("erro ao reposicionar dados");
    uint64_t meta[4] = {(uint64_t)file_id, count, (uint64_t)size, (uint64_t)error};
    MPI_Send(meta, 4, MPI_UINT64_T, 0, TAG_RESULT, MPI_COMM_WORLD);
    char buffer[CHUNK];
    uint64_t remaining = (uint64_t)size;
    while (remaining > 0) {
        int n = remaining < CHUNK ? (int)remaining : CHUNK;
        if (fread(buffer, 1, (size_t)n, out) != (size_t)n) die("falha ao ler resultado temporario");
        MPI_Send(buffer, n, MPI_BYTE, 0, TAG_DATA, MPI_COMM_WORLD);
        remaining -= (uint64_t)n;
    }
    fclose(out);
}
static void receive_result(int source, Result *results, int file_count) {
    uint64_t meta[4];
    MPI_Recv(meta, 4, MPI_UINT64_T, source, TAG_RESULT, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    if (meta[0] >= (uint64_t)file_count || results[meta[0]].data) die("resultado duplicado ou indice invalido");
    Result *r = &results[meta[0]];
    r->matches = meta[1];
    r->error = (int)meta[3];
    r->data = tmpfile();
    if (!r->data) die("falha ao criar resultado temporario no master");
    char buffer[CHUNK];
    uint64_t remaining = meta[2];
    while (remaining > 0) {
        int n = remaining < CHUNK ? (int)remaining : CHUNK;
        MPI_Recv(buffer, n, MPI_BYTE, source, TAG_DATA, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (fwrite(buffer, 1, (size_t)n, r->data) != (size_t)n) die("falha ao guardar resultado");
        remaining -= (uint64_t)n;
    }
    if (fflush(r->data) != 0) die("falha ao salvar resultado");
}
static void distribute_static(int files, int workers, int rank, int **assigned, int *nassigned) {
    if (rank == 0) {
        for (int dest = 1; dest <= workers; ++dest) {
            int n = files / workers + ((dest - 1) < files % workers ? 1 : 0);
            int *ids = xmalloc((size_t)n * sizeof(*ids));
            for (int k = 0; k < n; ++k) ids[k] = (dest - 1) + k * workers;
            MPI_Send(&n, 1, MPI_INT, dest, TAG_TASK, MPI_COMM_WORLD);
            MPI_Send(ids, n, MPI_INT, dest, TAG_TASK, MPI_COMM_WORLD);
            free(ids);
        }
    } else {
        MPI_Recv(nassigned, 1, MPI_INT, 0, TAG_TASK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        *assigned = xmalloc((size_t)(*nassigned) * sizeof(**assigned));
        MPI_Recv(*assigned, *nassigned, MPI_INT, 0, TAG_TASK, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
}
static void static_run(int rank, const FileList *files, const char *dir,
                       const char *term, Result *results, const int *assigned, int nassigned) {
    if (rank != 0) {
        for (int i = 0; i < nassigned; ++i) {
            int id = assigned[i];
            send_result(id, dir, files->names[id], term);
        }
    } else {
        for (int i = 0; i < files->count; ++i) {
            MPI_Status status;
            MPI_Probe(MPI_ANY_SOURCE, TAG_RESULT, MPI_COMM_WORLD, &status);
            receive_result(status.MPI_SOURCE, results, files->count);
        }
    }
}
static void dynamic_run(int rank, const FileList *files, const char *dir,
                        const char *term, Result *results, int workers) {
    if (rank != 0) {
        for (;;) {
            MPI_Send(NULL, 0, MPI_BYTE, 0, TAG_READY, MPI_COMM_WORLD);
            int id;
            MPI_Status status;
            MPI_Recv(&id, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            if (status.MPI_TAG == TAG_STOP) break;
            if (status.MPI_TAG != TAG_TASK || id < 0 || id >= files->count) die("tarefa invalida");
            send_result(id, dir, files->names[id], term);
        }
    } else {
        int next = 0, stopped = 0, completed = 0;
        while (stopped < workers) {
            MPI_Status status;
            MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            int source = status.MPI_SOURCE;
            if (status.MPI_TAG == TAG_READY) {
                MPI_Recv(NULL, 0, MPI_BYTE, source, TAG_READY, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                if (next < files->count) {
                    int id = next++;
                    MPI_Send(&id, 1, MPI_INT, source, TAG_TASK, MPI_COMM_WORLD);
                } else {
                    int unused = -1;
                    MPI_Send(&unused, 1, MPI_INT, source, TAG_STOP, MPI_COMM_WORLD);
                    ++stopped;
                }
            } else if (status.MPI_TAG == TAG_RESULT) {
                receive_result(source, results, files->count);
                ++completed;
            } else die("mensagem MPI inesperada");
        }
        if (completed != files->count) die("faltam resultados da busca");
    }
}
static int output_results(const FileList *files, Result *results, double elapsed) {
    uint64_t total = 0;
    int failed = 0;
    printf("=== RESULTADOS DA BUSCA ===\n");
    char buffer[CHUNK];
    for (int i = 0; i < files->count; ++i) {
        Result *r = &results[i];
        if (!r->data) die("arquivo sem resultado");
        if (fseeko(r->data, 0, SEEK_SET) != 0) die("erro ao ler resultado");
        size_t n;
        while ((n = fread(buffer, 1, sizeof(buffer), r->data)) > 0) {
            if (fwrite(buffer, 1, n, stdout) != n) die("erro ao imprimir saida");
        }
        if (ferror(r->data)) die("erro ao ler resultado");
        total += r->matches;
        failed |= r->error;
    }
    printf("\n=== RESUMO ===\nTotal de arquivos: %d\nTotal de ocorrencias: %" PRIu64 "\n",
           files->count, total);
    for (int i = 0; i < files->count; ++i) {
        printf("%s: %" PRIu64 " ocorrencias%s\n", files->names[i],
               results[i].matches, results[i].error ? " (ERRO DE LEITURA)" : "");
        fclose(results[i].data);
    }
    printf("Tempo de execucao: %.6f segundos\n", elapsed);
    if (failed) fprintf(stderr, "Aviso: ocorreram erros de leitura; resultados incompletos.\n");
    return failed;
}
static void usage(const char *argv0) {
    fprintf(stderr, "Uso: mpirun -np N %s --dir DIRETORIO --search TERMO --mode static|dynamic\n", argv0);
}
int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    int rank, processes;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &processes);
    const char *dir = NULL, *term = NULL, *mode = NULL;
    int valid = 1;
    for (int i = 1; i < argc; ++i) {
        if (i + 1 >= argc) { valid = 0; break; }
        const char *value = argv[++i];
        if (strcmp(argv[i - 1], "--dir") == 0 && !dir) dir = value;
        else if (strcmp(argv[i - 1], "--search") == 0 && !term) term = value;
        else if (strcmp(argv[i - 1], "--mode") == 0 && !mode) mode = value;
        else { valid = 0; break; }
    }
    if (!dir || !term || !*term || !mode ||
        (strcmp(mode, "static") != 0 && strcmp(mode, "dynamic") != 0) || processes < 2)
        valid = 0;
    if (!valid) {
        if (rank == 0) usage(argv[0]);
        MPI_Finalize();
        return EXIT_FAILURE;
    }
    FileList files = {0};
    if (rank == 0) files = find_logs(dir);
    files = share_list(files, rank);
    Result *results = rank == 0 ? calloc((size_t)(files.count ? files.count : 1), sizeof(*results)) : NULL;
    if (rank == 0 && !results) die("memoria insuficiente");
    int *assigned = NULL, nassigned = 0;
    int is_static = strcmp(mode, "static") == 0;
    MPI_Barrier(MPI_COMM_WORLD);
    double start = MPI_Wtime();
    if (is_static) {
        distribute_static(files.count, processes - 1, rank, &assigned, &nassigned);
        static_run(rank, &files, dir, term, results, assigned, nassigned);
    }
    else dynamic_run(rank, &files, dir, term, results, processes - 1);
    double elapsed = MPI_Wtime() - start;
    int failed = 0;
    if (rank == 0) failed = output_results(&files, results, elapsed);
    free(assigned);
    free(results);
    free_list(&files);
    MPI_Finalize();
    return failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
