# Idle memory cleanup

FlussoniX automatically attempts to return unused heap memory after media activity has been idle for five seconds. It runs once per idle period, with at least five minutes between attempts. It waits for worker retirement, retained response bodies, publications, pending authorization, startup and GPU checks. New media work waits asynchronously if cleanup has already begun.

The authenticated `/flussonix/api/v1/node` response includes `idle_memory_cleanup`: support and enabled status, active operations, attempt count, and the last completed duration/result. Reading status does not trigger cleanup. An attempt may return no memory when no unused pages are available.

Automatic cleanup is available for dynamically linked GNU/Linux builds using the system glibc allocator. Other allocator profiles remain disabled. No configuration change is required. The server does not change host allocator or kernel settings.
