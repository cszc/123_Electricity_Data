"""
Class for building suffix arrays from https://code.google.com/archive/p/pysuffix/
Code added for 123 is surrounded by 3 hash tags
Ultimately not needed and not used for tarzan algorithm.
"""

from sa_tools import *
from string import *

class Suffix_Array :

    def __init__(self, string):
        self.str_array = []
        self.suffix_array = []
        self.fusion = ''
        self.equiv = []
        #### These were added for 123
        self.string = str(string)
        self._add_str(self.string)
        self.karkkainen_sort()
        ###


    def _get_dict(self) :
        return {
          "str_array" : self.str_array,
          "suffix_array" : self.suffix_array
        }

    #### added for 123
    def get_suffixes(self):
        suffixes = set()
        for i in self.suffix_array:
            for j in self.suffix_array:
                if i == j:
                    continue
                suffixes.add(self.string[i:j+1])
        return list(suffixes)
    ###


    def _add_str(self, str_unicode) :
        taille = len(self.fusion)
        self.str_array.append(str_unicode)
        if taille != 0:
            self.fusion += chr(2)
            self.equiv.append(taille+1)
        else :
            self.equiv.append(0)
            self.fusion += str_unicode


    def karkkainen_sort(self) :
        n = len(self.fusion)
        s1 = self.fusion + chr(1) + chr(1) + chr(1)
        b = lst_char(s1)
        s2 = [0]*len(s1)
        kark_sort(s1,s2,n,b)
        self.suffix_array = s2
