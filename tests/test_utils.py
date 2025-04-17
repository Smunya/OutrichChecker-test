import unittest
from utils import normalize_text  # Імпортуємо функцію!

class TestNormalizeText(unittest.TestCase):

    def test_to_lower(self):
        self.assertEqual(normalize_text("TeSt"), "test")

    def test_remove_diacritics(self):
        self.assertEqual(normalize_text("тест з діакритикою"), "тест з диакритикою")
        self.assertEqual(normalize_text("ÁÉÍÓÚÜ"), "aeiouu")
        self.assertEqual(normalize_text("Çağırın Şemsileri"), "cagirin semsileri")  # Test Turkish chars

    def test_remove_extra_spaces(self):
        self.assertEqual(normalize_text("test   with   extra   spaces"), "test with extra spaces")
        self.assertEqual(normalize_text("  leading and trailing spaces  "), "leading and trailing spaces")
        self.assertEqual(normalize_text("multiple\nnewlines\n "), "multiple newlines ")

    def test_empty_string(self):
        self.assertEqual(normalize_text(""), "")

    def test_none_input(self):
        self.assertEqual(normalize_text(None), "")

    def test_integer_input(self):
        self.assertEqual(normalize_text(123), "123")

    def test_float_input(self):
        self.assertEqual(normalize_text(123.45), "123.45")

    def test_mixed_input(self):
        self.assertEqual(normalize_text("TeSt з діАкрИтИкоЮ  і  ПрОбІлАми\t123"),
                         "test з диакритикою и пробилами 123")
        self.assertEqual(normalize_text("  Test\nLine with\r\nCRLF and\tTabs  "),
                         "test line with crlf and tabs")

if __name__ == '__main__':
    unittest.main()
