# StringFileSorter

# Сортировка большого текстового файла, не влезающего в оперативную память. 
# 1. Устанавливаем максимальное количество символов в строке.
# 2. Устанавливает количество строк.
# 3. Получаем файл bigfile.txt
# 4. Разбиваем полученный файл на кусочки/очереди
# 5. Сортируем эти кусочки и соединяем в порядке: 
#     1 + 2 = 12
#     12 + 3 = 123 и т.д. (попарно).

