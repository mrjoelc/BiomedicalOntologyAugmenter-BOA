# BiomedicalOntologyAugmenter-BOA

## Installazione

Installare nel proprio sistema operativo Docker, al seguente link è possibile procedere con il download. 
In seguito avviare Docker. 
Effettuare una clone del repository git mediante il comando:
git clone https://github.com/mrjoelc/BiomedicalOntologyExtractor-BOA
Recarsi nelle cartelle corrispondenti ai microservizi e costruire l’immagine del microservizio mediante il comando da terminale:
docker build -t nome_microservizio:tag  .    (1)
Recarsi nella root del progetto e avviare il comando docker-compose:
docker-compose -f docker-compose.yml up  (2)

## Effettuare una ricerca

Recarsi all’indirizzo localhost:8080/.  Si presenterà una interfaccia dove si potrà inserire una keyword e ricercare nell’ontologia una corrispondenza. Se nessuna keyword viene fornita verranno mostrati tutte le classi presenti nell’ontologia.
Selezionare le classi per cui si vuole effettuare una ricerca. E’ possibile selezionarle alcune o tutte in un colpo.
Selezionare un repository (default PubMed) e il limit (default 20) inviare la richiesta al repository.
Si verrà rimandati alla seguente pagine che mostra alcune info riguardo le classi per cui si è scelto di effettuare la ricerca e il repository selezionato


## Visualizzare i risultati
Recarsi da browser web all’indirizzo: http://localhost:8081/db/boe_database/results
Si presenterà una interfaccia MongoDB Express dove è possibile visualizzare le entry ed eventualmente effettuare un dump dell’intera collezione in formato json.


## Spegnere il servizio

Recarsi nel terminale in cui è stato avviato il comando docker-compose
Fermare il servizio con CTRL+c x2
Utilizzare il seguente comando:
docker-compose -f docker-compose.yml down


## Informazioni aggiuntive

(1) Per alcuni microservizi relativi ai repository potrebbe essere necessaria una modifica per far correttamente funzionare le API. Ad esempio nel microservizio Searcher_PubMed e’ stata necessaria una registrazione di un account standard al sito PubMed.com ed in seguito definita qual è l’email e il nome del servizio appropriato.
(2) Nella root sono presenti due file docker-compose: uno di sviluppo (con solo kafka, zookeeper, mongoDB e mongo-express agganciati a localhost) e uno completo di deploy in modo tale che si possa eseguire il codice python in fase di sviluppo esternamente ai container e avere kafka e mongoDB sempre attivo: questo ai fini di velocizzare le fasi di sviluppo. 

