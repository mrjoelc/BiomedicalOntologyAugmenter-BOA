# BiomedicalOntologyAugmenter-BOA

## Installazione

Installare nel proprio sistema operativo Docker, al seguente link è possibile procedere con il download. 
In seguito avviare Docker. 
Effettuare una clone del repository git mediante il comando:
git clone https://github.com/mrjoelc/BiomedicalOntologyExtractor-BOA
Recarsi nelle cartelle corrispondenti ai microservizi e costruire l’immagine del microservizio mediante il comando da terminale:
docker build -t nome_microservizio:tag  .    (*)
Recarsi nella root del progetto e avviare il comando docker-compose:
docker-compose -f docker-compose.yml up  (**)
