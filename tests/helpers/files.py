import os


def get_file_names(directory_path: str):
    file_names = []

    for item in os.listdir(directory_path):
        item_path = os.path.join(directory_path, item)

        if os.path.isfile(item_path):
            file_names.append(item)

    return file_names
