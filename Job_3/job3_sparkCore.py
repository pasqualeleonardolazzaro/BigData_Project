from pyspark import SparkContext
sc = SparkContext.getOrCreate()

def parse_row(row):
    fields = row.split(',')
    try:
        return (
            (int(fields[10][:4]), fields[1]),  # key: (year, ticker)
            (fields[10], float(fields[6]), fields[2])  # value: (date, close, name)
        )
    except ValueError:
        return None

# Caricare i dati
rdd = sc.textFile("file:///home/paleo/BigData/2Proj/Dataset/out.csv")

# Rimuovere l'intestazione
header = rdd.first()
filtered_rdd = rdd.filter(lambda row: row != header)

# Parsing e raggruppamento per (anno, ticker)
grouped_data = filtered_rdd.map(parse_row).filter(lambda x: x is not None and x[0][0] >= 2000).groupByKey()



# Calcolare la variazione percentuale per ogni (anno, ticker)
def compute_percentage(changes):
    changes_list_unsorted= list(changes)
    changes_list = sorted(changes_list_unsorted, key=lambda x: x[0])
    if len(changes_list) > 1:
        start_price = changes_list[0][1]
        end_price = changes_list[-1][1]
        percentage_change = ((end_price - start_price) / start_price) * 100
        return (changes_list[0][2], format(percentage_change, ".1f"))  # (name, percentage_change)
    return (changes_list[0][2], None)

trend_rdd = grouped_data.mapValues(compute_percentage).filter(lambda x: x[1][1] is not None)


# Creazione del pattern per 3 anni consecutivi con lo stesso trend
def to_pattern(data):
    sorted_data = sorted(data)
    result = []
    for i in range(len(sorted_data)-2):
        year1, year2, year3 = sorted_data[i][0], sorted_data[i+1][0], sorted_data[i+2][0]
        if year3 - year1 == 2:  # Controlla se gli anni sono consecutivi
            p1, p2, p3 = sorted_data[i][1], sorted_data[i+1][1], sorted_data[i+2][1]
            result.append(((year1, year2, year3, p1, p2, p3), sorted_data[i][2]))
    return result

# Raggruppare per ticker
trend_groups = trend_rdd.map(lambda x: (x[0][1], (x[0][0], x[1][1], x[1][0]))).groupByKey().flatMap(lambda x: to_pattern(x[1])).groupByKey()



# Filtrare e stampare i risultati per pattern con più di un ticker
final_result = trend_groups.filter(lambda x: len(x[1]) > 1).collect()


output_path = "/home/paleo/BigData/2Proj/Dataset/Job3_spark/output_sparkCore.txt"

with open(output_path, 'w') as file:
    for pattern, tickers in final_result:
        file.write(f"Companies {list(tickers)} share the same trend [{pattern[3]}, {pattern[4]}, {pattern[5]}] in years [{pattern[0]}, {pattern[1]}, {pattern[2]}]\n")

sc.stop()


