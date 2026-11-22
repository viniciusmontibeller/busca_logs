# INE5645 — Busca distribuída em logs

Aplicação em **C com MPI** para buscar um termo em arquivos `.log`, comparando distribuição **estática** e **dinâmica** de tarefas no padrão master–worker.

## Compilação

Requer compilador C e uma implementação MPI (por exemplo, Open MPI).

```bash
mpicc -O3 -Wall -Wextra -std=c11 busca_logs.c -o busca_logs
gcc -O2 -Wall -Wextra gerador_logs_t2.c -o gerador_logs_t2
```

Para testar o envio estático em blocos com um limite artificial de 4 KiB:

```bash
mpicc -O3 -Wall -Wextra -std=c11 -DLIMITE_MENSAGEM_STATIC=4096 busca_logs.c -o busca_logs_bloco
```

## Geração e execução local

```bash
./gerador_logs_t2 100 10000 8 42 logs

mpirun -np 5 ./busca_logs --dir logs --search ERROR --mode static
mpirun -np 5 ./busca_logs --dir logs --search ERROR --mode dynamic
```

`-np 5` corresponde a **1 coordenador e 4 workers**. São necessários pelo menos dois processos MPI. Todos os processos devem acessar o mesmo diretório de arquivos.

## Implementação

- Cada processo lista e ordena os arquivos `.log` do diretório; cada arquivo é uma tarefa indivisível.
- **Estática:** o master distribui previamente os índices de forma cíclica. Cada worker devolve seus resultados em uma mensagem empacotada (`MPI_Pack`) ou, se exceder o limite definido, envia metadados e texto em blocos.
- **Dinâmica:** o master atribui um arquivo por vez e envia uma nova tarefa assim que recebe o resultado do worker; usa `-1` para encerrar.
- A busca utiliza `getline` e `strstr`, diferenciando maiúsculas e minúsculas. Cada linha encontrada conta uma vez, mesmo com múltiplas ocorrências do termo.
- O master consolida a saída em ordem de arquivo e número de linha, com total por arquivo, total geral e tempo.
- O tempo medido com `MPI_Wtime` inclui distribuição, busca e comunicação dos resultados, mas **não** a listagem inicial dos arquivos nem a impressão final.

## Execução no Santos Dumont

Código e scripts `.srm` ficam na **HOME** (`~/busca_logs_t2`); executáveis, logs de entrada e resultados ficam no **SCRATCH** (`$SCRATCH/busca_logs_t2`). Após compilar, copie o executável para o SCRATCH.

```bash
module load openmpi/gnu/4.1.4_sequana
cd ~/busca_logs_t2
mpicc -O3 -Wall -Wextra -std=c11 busca_logs.c -o busca_logs
cp busca_logs "$SCRATCH/busca_logs_t2/"

cd "$SCRATCH/busca_logs_t2"
sbatch --nodes=1 --ntasks=4 --ntasks-per-node=4 \
  --export=ALL,DATASET=logs,TERMO=ERROR,REPETICOES=5 \
  "$HOME/busca_logs_t2/job_benchmark.srm"
```

O script `job_gerador.srm` gera os datasets; `job_busca.srm` executa uma estratégia isoladamente; `job_benchmark.srm` alterna as estratégias, compara as saídas e registra os tempos em `resultados/tempos-<job>.csv`. As execuções MPI nos scripts usam `mpirun` dentro da alocação Slurm.

## Experimentos e limitações

Os testes no Santos Dumont utilizaram **100, 500, 1.000 e 5.000 arquivos**, com **4 a 96 processos MPI** e até **4 nós**, repetindo as buscas cinco vezes por configuração. Os resultados permitem comparar tempo, variação entre repetições e escalabilidade.

Os resultados são acumulados em memória antes do envio; a dinâmica envia um arquivo por mensagem e a estática pode dividir lotes grandes em blocos. O limite normal para o pacote estático é `INT_MAX - 16 MiB`. A listagem inicial, realizada por todos os processos, não entra no tempo medido. Para avaliar o tempo total do programa, é necessária uma medição adicional de ponta a ponta.
