/* Memory balloon (from Shiva): allocate and mlock a large block so the OS
 * page cache and free memory shrink, making disk I/O real instead of
 * cache-served. Size below is 50 GB (the original file's comment said 16 GB;
 * the code always allocated 50 - kept as given, adjust alloc_size as needed).
 * Build: gcc -O2 -o balloon balloon.c
 * Run:   sudo ./balloon        (mlock beyond RLIMIT_MEMLOCK needs root,
 *                               or: sudo ulimit adjustment / setcap)
 * Stop:  Ctrl+C or kill - the memory frees on exit. */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/mman.h>

int main() {
    long int alloc_size = 50L * 1024 * 1024 * 1024;  // 50 GB
    char *memory = (char *)malloc(alloc_size);

    if (memory == NULL) {
        perror("Memory allocation failed");
        exit(EXIT_FAILURE);
    }

    int err = mlock(memory, alloc_size);

    if (err == 0) {
        printf("Memory successfully allocated and locked in RAM.\n");
    } else {
        perror("mlock failed");
        exit(EXIT_FAILURE);
    }

    while (1) {
        sleep(10000);
    }

    free(memory);
    return 0;
}
