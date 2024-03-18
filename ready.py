import json
from pathlib import Path

def mapper(path):
    n, mean, M2 = 0, 0.0, 0.0

    # Проверяем, является ли путь файлом и имеет ли расширение .json
    if path.is_file() and path.suffix == '.json':
        with open(path, 'r') as f:
            info = json.load(f)
        score = float(info.get('movieIMDbRating', '0.0'))
        n += 1
        delta = score - mean
        mean += delta / n
        M2 += delta * (score - mean)

    return (n, mean, M2)

def reducer(score_data1, score_data2):
    # Распаковываем данные из кортежей
    n1, mean1, M21 = score_data1
    n2, mean2, M22 = score_data2

    # Обновляем суммы для объединения данных
    delta = mean2 - mean1
    mean = (n1 * mean1 + n2 * mean2) / (n1 + n2)
    M2 = M21 + M22 + delta * delta * n1 * n2 / (n1 + n2)

    # Обновляем общее количество записей
    n = n1 + n2

    return n, mean, M2

# Производим импорт нужных модулей
from functools import reduce

# Запускаем процесс MapReduce
score_data = map(mapper, Path('imdb-user-reviews').glob('**/*'))
n, mean, M2 = reduce(reducer, score_data)

# Выводим результат
print("Mean IMDb Rating:", mean)
print("Standard Deviation of IMDb Ratings:", (M2 / n) ** 0.5)
