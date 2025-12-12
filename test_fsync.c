#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Usage: %s <file_path>\n", argv[0]);
        return 1;
    }

    const char *path = argv[1];
    
    // Open file
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        perror("open");
        return 1;
    }

    // Write data
    const char *data = "Testing fsync functionality!\n";
    ssize_t written = write(fd, data, strlen(data));
    if (written < 0) {
        perror("write");
        close(fd);
        return 1;
    }

    printf("Wrote %zd bytes to %s\n", written, path);

    // Call fsync to force cache flush
    printf("Calling fsync()...\n");
    if (fsync(fd) < 0) {
        perror("fsync");
        close(fd);
        return 1;
    }

    printf("✓ fsync() completed successfully\n");
    printf("✓ Data should now be on disk\n");

    close(fd);
    return 0;
}
