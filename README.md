# Progetto di Scalable and Cloud Programming: Analisi di Coacquisto
Davide Cristoni, matricola 0001123518, Anno Accademico 24-25

## Comandi utili

Regioni Italiane:

| zona          | regioni | location       |
|---------------|---------|----------------|
| europe-west8  | a, b, c | Milano, Italia |
| europe-west12 | a, b, c | Torino, Italia |



Regioni a ridotto impatto ambientale situate in Europa:

| zona                                                                    | location               |
|-------------------------------------------------------------------------|------------------------|
| europe-north1-a <br/> europe-north1-b <br/> europe-north1- c            | Hamina, Finlandia      |
| europe-north2-a <br/> europe-north2-b <br/> europe-north2-c             | Stoccolma, Svezia      |
| europe-southwest1-a <br/> europe-southwest1-b <br/> europe-southwest1-c | Madrid, Spagna         |
| europe-west1-b <br/> europe-west1-c <br/> europe-west1-d                | San Ghislain, Belgio   |
| europe-west2-a <br/> europe-west2-b <br/> europe-west2-c                | Londra, Inghilterra    |
| europe-west3-a <br/> europe-west3-b <br/> europe-west3-c                | Francoforte, Germania  |
| europe-west4-a <br/> europe-west4-b <br/> europe-west4-c                | Eemshaven, Paesi Bassi |
| europe-west6-a <br/> europe-west6-b <br/> europe-west6-c                | Zurigo, Svizzera       |
| europe-west9-a <br/> europe-west9-b <br/> europe-west9-c                | Parigi, Francia        |


### Creazione cluster
Per la creazione di un cluster con un singolo nodo:
```shell
gcloud dataproc clusters create <nome> --single-node --region=<regione> --master-boot-disk-size 240 --worker-boot-disk-size 240 --image-version <num-immagine>
```
Per la creazione di un cluster con più nodi:
```shell
gcloud dataproc clusters create <nome> --region=<regione> --num-workers <n> --master-boot-disk-size 240 --worker-boot-disk-size 240 --image-version 2.3
```

### Creazione bucket
```shell
gsutil mb -l <regione> gs://<nome-bucket-unico>/
```
mb -> make bucket
-l -> specifica la regione
il nome deve essere tutto minuscolo e senza spazi, oltre che univoco a livello globale

### Otteni nome bucket
```shell
gcloud storage buckets list
```

### Copia file
```shell
gsutil cp <nome-file> gs://<nome-bucket>/<nome-file>
```
### Immissione job

```shell
gcloud dataproc jobs submit spark --cluster=<nome> --region=<regione> --jar=gs://<bucket>/<nome-jar>.jar -- <parametro-1> <parametro-2>
```

### Cancellazione cluster
**IMPORTANTE: è fondamentale cancellare il cluster dopo il suo utilizzo per 
evitare la consumazione inutile di crediti.**
```shell
gcloud dataproc clusters delete <nome> --region <regione>
```

## Tempi di esecuzione su europe-west1 con n1-standard-4

| tempo (ns)    | tempo (sec) | num nodi | speedup | scaling |
|---------------|-------------|----------|---------|---------|
| 718744825489  | 718         | 1        | 1       | 1       |
| 324873888123  | 324         | 2        | 2,22    | 1,11    |
| 257254619445  | 257         | 3        | 2.79    | 0,93    |
| 219754301113  | 219         | 4        | 3,28    | 0,82    |

### Copia incolla per i comandi
gcloud dataproc clusters create scp --region europe-west9 --zone europe-west9-a --master-machine-type n4-standard-2 --master-boot-disk-size 100g --num-workers 2 --worker-machine-type n4-standard-2 --worker-boot-disk-size 100 --image-version 2.3

gcloud dataproc jobs submit spark --cluster=scp --region=europe-west9 --jar=gs://scp-davide-copurchase/scp-copur.jar -- gs://scp-davide-copurchase/input.csv gs://scp-davide-copurchase/output

### Commenti di sviluppo
Il file è uno e va letto -> collo di bottiglia. => va partizionato
