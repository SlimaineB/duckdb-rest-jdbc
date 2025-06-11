# Intégration DuckDB JDBC avec Tableau Desktop

## Prérequis

- Tableau Desktop installé (exemple : `C:\Program Files\Tableau\Tableau 2025.1\bin\tableau.exe`)
- Driver JDBC DuckDB
- Fichier du connecteur `.taco` généré

## Étapes d'installation

1. **Désactiver la vérification de la signature des connecteurs**  
   Lancez Tableau Desktop avec l’option suivante :
   ```sh
   "C:\Program Files\Tableau\Tableau 2025.1\bin\tableau.exe" -DDisableVerifyConnectorPluginSignature=true
   ```

2. **Installer le driver JDBC**  
   Copiez le fichier du driver JDBC dans le dossier suivant :
   ```
   C:\Program Files\Tableau\Drivers
   ```

3. **Préparer le connecteur .taco**  
   - Compressez le dossier `slimdb_jdbc-v1.0.0-taco` au format ZIP.
   - Renommez l’extension `.zip` en `.taco`.

4. **Installer le connecteur**  
   Copiez le fichier `.taco` dans le dossier :
   ```
   C:\Program Files\Tableau\Connectors
   ```

## Remarques

- Redémarrez Tableau Desktop après l’installation du connecteur.
- Vérifiez que le connecteur apparaît dans la liste des sources de données.

---

