# DuckDB JDBC Integration with Tableau Desktop

## Prerequisites

- Tableau Desktop installed (example: `C:\Program Files\Tableau\Tableau 2025.1\bin\tableau.exe`)
- DuckDB JDBC driver
- Generated `.taco` connector file

## Installation Steps

1. **Disable connector signature verification**  
   Launch Tableau Desktop with the following option:
   ```sh
   "C:\Program Files\Tableau\Tableau 2025.1\bin\tableau.exe" -DDisableVerifyConnectorPluginSignature=true
   ```

2. **Install the JDBC driver**  
   Copy the JDBC driver file to the following folder:
   ```
   C:\Program Files\Tableau\Drivers
   ```

3. **Prepare the .taco connector**  
   - Compress the `slimdb_jdbc-v1.0.0-taco` folder as a ZIP file.
   - Rename the `.zip` extension to `.taco`.

4. **Install the connector**  
   Copy the `.taco` file to the following folder:
   ```
   C:\Program Files\Tableau\Connectors
   ```

## Notes

- Restart Tableau Desktop after installing the connector.
- Check that the connector appears in the list of data sources.
- Tableau logs are available at:  
  ```
  C:\Users\<account_name>\Documents\My Tableau Repository\Logs
  ```

---

