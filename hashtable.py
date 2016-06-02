# CS122 W'16: Markov models and hash tables
# Christine Chung

import sys
import math
import Hash_Table

HASH_CELLS = 57

class Markov:

    def __init__(self, k, s):
        '''
        Constructs a new k-order Markov model using the statistics of string "s"
        '''
        self.hash_table = Hash_Table.Hash_Table(HASH_CELLS, 0)
        self.alpha_size = len(set(s))
        self._build_hash_table(k, s)
        self.k = k


    def _build_hash_table(self, k, string):
        '''
        Iterates through k and k+1 length substrings in string to construct a new
        hash table object.
        '''
        string = string[-k:] + string
        for i in range(len(string)- k):
            for j in range(2):
                slice_len = k + j + i
                key = string[i:slice_len]
                val = self.hash_table.lookup(key) + 1
                self.hash_table.update(key, val)
	# GRADER COMMENT
	# PENALTY: -5
	# in the case k = 0, when i,j are both 0, key will be an empty string, but you started indexing this string without checking this in _get_hash(key) function. so an exception was thrown out.


    def log_probability(self, string):
        '''
        Get the log probability of string "s", given the statistics of
        character sequences modeled by this particular Markov model
        '''
        numerator = 0
        denominator = 0
        probability = 0
        string = string[-self.k:] + string

        for i in range(len(string) - self.k):
            slice_len = self.k + i + 1
            key = string[i:slice_len]
            probability += self.get_probability(key)

        return probability


    def get_probability(self, s):
        '''
        Takes a substring 's' of length k + 1 and looks up string(k) and string(k + 1)
        and returns individual log probability for this particular Markov Model.
        '''
        numerator = self.hash_table.lookup(s) + 1
        denominator = self.hash_table.lookup(s[:-1]) + self.alpha_size
        probability = math.log(numerator / denominator)
        return probability


def identify_speaker(speech1, speech2, speech3, order):
    '''
    Given sample text from two speakers, and text from an unidentified speaker,
    return a tuple with the normalized log probabilities of each of the speakers
    uttering that text under a "order" order character-based Markov model,
    and a conclusion of which speaker uttered the unidentified text
    based on the two probabilities.
    '''
    markov_A = Markov(order, speech1)
    markov_B = Markov(order, speech2)
    normalizer = len(speech3)
    probabilityA = markov_A.log_probability(speech3) / normalizer
    probabilityB = markov_B.log_probability(speech3) / normalizer
    result = 'A' if probabilityA > probabilityB else 'B'
    return(probabilityA, probabilityB, result)



def print_results(res_tuple):
    '''
    Given a tuple from identify_speaker, print formatted results to the screen
    '''
    (likelihood1, likelihood2, conclusion) = res_tuple

    print("Speaker A: " + str(likelihood1))
    print("Speaker B: " + str(likelihood2))

    print("")

    print("Conclusion: Speaker " + conclusion + " is most likely")


if __name__=="__main__":
    num_args = len(sys.argv)

    if num_args != 5:
        print("usage: python3 " + sys.argv[0] + " <file name for speaker A> " +
              "<file name for speaker B>\n  <file name of text to identify> " +
              "<order>")
        sys.exit(0)

    with open(sys.argv[1], "rU") as file1:
        speech1 = file1.read()

    with open(sys.argv[2], "rU") as file2:
        speech2 = file2.read()

    with open(sys.argv[3], "rU") as file3:
        speech3 = file3.read()

    res_tuple = identify_speaker(speech1, speech2, speech3, int(sys.argv[4]))

    print_results(res_tuple)
