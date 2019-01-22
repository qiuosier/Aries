import gzip


def unzip_gz_file(file_path):
    """Un-zips a .gz file within the same directory.

    Args:
        file_path (str): the file to be unzipped.

    Returns:
        The output filename with full path.
    """
    output_file = file_path[:-3]
    with gzip.open(file_path, 'rb') as gzip_file:
        with open(output_file, "wb") as unzipped_file:
            print("Unzipping %s..." % file_path.split("/")[-1])
            block_size = 1 << 20
            while True:
                block = gzip_file.read(block_size)
                if not block:
                    break
                unzipped_file.write(block)
    return output_file
