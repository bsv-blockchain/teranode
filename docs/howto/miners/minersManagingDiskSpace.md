# Managing Disk Space - Teranode

Key considerations and strategies:


1. **Monitoring Disk Usage:**
- Regularly check available disk space using tools like `df -h` or through the Grafana dashboard.
- Set up alerts to notify you when disk usage reaches certain thresholds (e.g., 80% or 85% full).

2. **Understanding Data Growth:**
- The blockchain data, transaction store, and subtree store will grow over time as new blocks are added to the network.
- Growth rate depends on network activity and can vary significantly.

3. **Pruning Strategies:**
- Teranode implements regular pruning of old data that's no longer needed for immediate operations.
- While retention policies for different data types are configurable, this is not documented in this guide.

7. **Log Management:**
- Consider offloading logs to a separate storage system or log management service.

10. **Backup and Recovery:**
- Implement a backup strategy that doesn't interfere with disk space management.
- Ensure backups are stored on separate physical media or cloud storage.

11. **Performance Impact:**
- Be aware that very high disk usage (>90%) can negatively impact performance.
- Monitor I/O performance alongside disk space usage.
