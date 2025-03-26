#include <stdio.h>
#include <stdlib.h>

#define LOG_FILE "wal.log"
#define MAIN_FILE "main.data"
#define CHECKPOINT_INTERVAL 5 // Number of operations before checkpoint

typedef struct {
    int operation; // 1 write 2 delete
    int offset;
    char data[256];
} wal_entry;

void write_to_log(wal_entry* entry) {
    FILE* log_file = fopen(LOG_FILE, "ab");
    if (!log_file) {
        perror("Error while opening a log file\n");
        exit(EXIT_FAILURE);
    }
    fwrite(entry, sizeof(wal_entry), 1, log_file);
    fclose(log_file);
}

void apply_log_to_main() {
    FILE* log_file = fopen(LOG_FILE, "rb");
    if (!log_file) {
        perror("No log file to apply\n");
        return;
    }

    wal_entry entry;
    while (fread(&entry, sizeof(wal_entry), 1, log_file)) {
        FILE* main_file = fopen(MAIN_FILE, "r+b");
        if (!main_file) {
            perror("Failed to open open file");
            exit(EXIT_FAILURE);
        }
        fseek(main_file, entry.offset, SEEK_SET);
        fwrite(entry.data, sizeof(entry.data), 1, main_file);
        fclose(main_file);
    }
    fclose(log_file);

    remove(LOG_FILE);
}

int main() {
    int operations = 0;
    while (1) {
        wal_entry entry;
        printf("Write operation: 1 - write, 2 - delete: ");
        scanf("%d", &entry.operation);
        printf("Enter offset: ");
        scanf("%d", &entry.offset);
        printf("Enter data: ");
        scanf("%s", entry.data);

        write_to_log(&entry);
        operations++;

        if (operations >= CHECKPOINT_INTERVAL) {
            apply_log_to_main();
            operations = 0;
        }
    }
    return EXIT_SUCCESS;
}
