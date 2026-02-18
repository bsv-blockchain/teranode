# Updating Teranode to a New Version - Docker Compose

1. Download and copy any newer version of the **docker compose file**, if available, into your project repository.

2. **Update Docker Images**

    Pull the latest versions of all images:

    ```bash
    docker compose pull
    ```

3. **Stop the Current Stack**

    ```bash
    docker compose down
    ```

4. **Start the Updated Stack**

    ```bash
    docker compose up -d
    ```

5. **Verify the Update**

    Check that all services are running and healthy:

    ```bash
    docker compose ps
    ```

6. **Check Logs for Any Issues**

    ```bash
    docker compose logs
    ```



**Important Considerations:**

- **Data Persistence**: The update process should not affect data stored in volumes (./data directory). However, it's always good practice to backup important data before updating.
- **Configuration Changes**: Check the release notes or documentation for any required changes to `settings_local.conf` or environment variables.
- **Database Migrations**: Some updates may require database schema changes. Where applicable, this will be handled transparently by docker compose.
- **Downtime**: Be aware that this process involves stopping and restarting all services, which will result in some downtime. Rolling updates are not possible with the `docker compose` setup.



**After the update:**

- Monitor the system closely for any unexpected behavior.
- Check the Grafana dashboards to verify that performance metrics are normal.



**If you encounter any issues during or after the update, you may need to:**

- Check the specific service logs for error messages.
- Consult the release notes or documentation for known issues and solutions.
- Reach out to the Teranode support team for assistance.



Remember, the exact update process may vary depending on the specific changes in each new version. Always refer to the official update instructions provided with each new release for the most accurate and up-to-date information.
