# Busca distribuida em logs

Solucao didatica em C/MPI com distribuicao estatica ciclica e distribuicao dinamica master-worker.

## Compilacao

Requer implementacao MPI (OpenMPI ou MPICH) com `mpicc` e `mpirun`:

```sh
make
```

## Execucao

```sh
mpirun -np 5 ./busca_logs --dir logs --search ERROR --mode static
mpirun -np 5 ./busca_logs --dir logs --search ERROR --mode dynamic
```

`-np 5` representa 1 master (rank 0) e 4 workers. Sao necessarios pelo menos 2 processos.
O parametro `--search` aceita sequencias, inclusive com espacos se colocadas entre aspas.

## Decisoes de implementacao

- O rank 0 enumera arquivos regulares com extensao `.log`, sem recursao, e ordena nomes com `strcmp`.
- A lista de nomes e transmitida aos workers uma unica vez via `MPI_Bcast`.
- Os processos abrem diretamente os arquivos a partir do diretorio indicado. **Todos precisam enxergar os mesmos arquivos e caminho** (por exemplo, filesystem compartilhado do cluster).
- Cada arquivo e uma tarefa indivisivel.
- Estrategia estatica ciclica: arquivo de indice i vai para o worker `(i % W) + 1`. Cada worker recebe sua lista completa antes de iniciar a busca; a medicao inclui essa distribuicao.
- Estrategia dinamica: workers enviam READY, recebem TASK, enviam RESULT e voltam a enviar READY; STOP encerra cada worker.
- A busca usa `getline` e `strstr` (case-sensitive); cada linha correspondente conta uma unica vez.
- Resultados sao escritos em arquivos temporarios e enviados ao master em blocos de ate 64 KiB, para evitar alocar todas as linhas encontradas na memoria.
- O master armazena resultados separados por arquivo e imprime na ordem de nome e linha.
- `MPI_Wtime` mede distribuicao das tarefas/procura/transmissao/consolidacao em arquivos temporarios; nao inclui listagem inicial, broadcast inicial da lista de nomes, nem impressao final no terminal.
- Arquivos que nao puderem ser lidos sao assinalados e a execucao termina com codigo de erro.

## Testes sugeridos

```sh
mkdir -p logs
printf 'INFO ok\nERROR falha\nERROR ERROR repetido\n' > logs/a.log
printf 'WARNING ok\nERROR segunda\n' > logs/b.log
mpirun -np 3 ./busca_logs --dir logs --search ERROR --mode static > static.txt
mpirun -np 3 ./busca_logs --dir logs --search ERROR --mode dynamic > dynamic.txt
# Compare o trecho de resultados e os totais; o tempo ira diferir.
```

## Avaliacao experimental

Execute ambas as estrategias com as mesmas entradas, para 2, 4, 8 workers, repetindo 5 vezes cada caso. Teste arquivos similares e arquivos de tamanhos diferentes. Registre tempos e compare speedup relativo, media, variabilidade e custo de comunicacao. Para medir speedup contra um programa sequencial, implemente tambem um baseline sequencial equivalente (nao use a execucao MPI com um worker como substituto silencioso).

## Observacoes

- Utiliza POSIX (`getline`, `tmpfile`, `fseeko`, `ftello`) e MPI com `MPI_UINT64_T`.
- Nao faz busca em subdiretorios; nao processa arquivos sem a extensao `.log`.
- Resultados temporarios requerem espaco em disco e o master precisa de espaco proporcional ao volume total de linhas encontradas.
- Para comparacoes de tempo com muitos matches, redirecione a saida para arquivo; o tempo exibido nao inclui a escrita final da saida.
- A versao foi preparada para compilacao com MPI, mas o ambiente de autoria nao dispunha de `mpicc`/`mpirun`; valide no ambiente de execucao do grupo antes da entrega.
