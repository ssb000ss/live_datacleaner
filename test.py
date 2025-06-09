from transliterate import translit
from unidecode import unidecode
from utils.data_cleaner import PatternDetector
import ftfy

detector = PatternDetector()
a = 'colа'
b = 'Кoля'
c = 'Жeня'

new_a = unidecode(a)
new_b = unidecode(b)
new_c = unidecode(c)

print(detector.detect_patterns([a, ]), detector.detect_patterns([new_a,]), new_a)
print(detector.detect_patterns([b, ]), detector.detect_patterns([new_b,]), new_b)
print(detector.detect_patterns([c, ]), detector.detect_patterns([new_c,]), new_c)

# print(translit(new_a, "ru"))
# print(translit(new_b, "ru"))
# print(translit(new_c, "ru"))

print(translit(a, "ru"))
print(translit(b, "ru"))
print(translit(c, "ru"))