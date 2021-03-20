import csv


def csv2json(file_path):
    """
    Reads CSV file and transforms rows to JSON format
    """

    data = []
    with open(file_path) as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            data.append(row)
    csv_file.close()

    return data
