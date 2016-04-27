# Storm - Contador de números

Para poder compilar el proyecto se debe realizar

	$ mvn clean package

Posteriormente, para ejecutarlo en Storm, se debe realizar lo siguiente:

	$ storm jar countNumber-2.0.0-SNAPSHOT.jar citiaps.countApp.Topology countNumber

