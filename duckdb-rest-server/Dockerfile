# Utilise une image Java officielle pour exécuter Spring Boot
FROM eclipse-temurin:17-jre

# Répertoire de travail dans le conteneur
WORKDIR /app

# Copie le jar généré dans le conteneur
COPY target/*.jar app.jar

# Expose le port par défaut de Spring Boot
EXPOSE 8080

# Commande pour lancer l'application
ENTRYPOINT ["java", "-jar", "app.jar"]