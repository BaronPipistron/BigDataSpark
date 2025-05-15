import re

def clean_numbers_in_file(input_file, output_file):
    """
    Читает файл input_file, соединяет разбитые на несколько строк CSV‑записи,
    убирает лишние пробелы, и сохраняет полноценный CSV в output_file.
    Каждая запись будет на отдельной строке.
    """
    new_rec = re.compile(r'^\d+,')

    cleaned_lines = []
    temp_line = ""

    with open(input_file, 'r', encoding='utf-8') as fin:
        for raw_line in fin:
            line = raw_line.rstrip('\r\n')
            line = re.sub(r'\s{2,}', ' ', line)
            line = re.sub(r'\s*,\s*', ',', line)
            line = re.sub(r'\s*=\s*', '=', line)

            if new_rec.match(line):
                if temp_line:
                    cleaned_lines.append(temp_line)
                temp_line = line
            else:
                temp_line += line

        if temp_line:
            cleaned_lines.append(temp_line)

    with open(output_file, 'w', encoding='utf-8', newline='\n') as fout:
        fout.write("\n".join(cleaned_lines))

files = [
    ('../raw/MOCK_DATA (1).csv', '../prepared/mock1.csv'),
    ('../raw/MOCK_DATA (2).csv', '../prepared/mock2.csv'),
    ('../raw/MOCK_DATA (3).csv', '../prepared/mock3.csv'),
    ('../raw/MOCK_DATA (4).csv', '../prepared/mock4.csv'),
    ('../raw/MOCK_DATA (5).csv', '../prepared/mock5.csv'),
    ('../raw/MOCK_DATA (6).csv', '../prepared/mock6.csv'),
    ('../raw/MOCK_DATA (7).csv', '../prepared/mock7.csv'),
    ('../raw/MOCK_DATA (8).csv', '../prepared/mock8.csv'),
    ('../raw/MOCK_DATA (9).csv', '../prepared/mock9.csv'),
    ('../raw/MOCK_DATA.csv',    '../prepared/mock10.csv'),
]

for inp, out in files:
    print(f"Обработка {inp} → {out}")
    clean_numbers_in_file(inp, out)
print("Готово!")
