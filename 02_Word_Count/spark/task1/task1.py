from pyspark import SparkContext, SparkConf
import re

# count words without in stop_words, numbers 
stop_words = ["a", "able", "about", "across", "after", "all", "almost", "also", "am", "among", "an", "and", "any", "are", "as", "at", "be", "because", "been", "but", "by", "can", "cannot", "could", "dear", "did", "do", "does", "either", "else", "ever", "every", "for", "from", "get", "got", "had", "has", "have", "he", "her", "hers", "him", "his", "how", "however", "i", "if", "in", "into", "is", "it", "its", "just", "least", "let", "like", "likely", "may", "me", "might", "most", "must", "my", "neither", "no", "nor", "not", "of", "off", "often", "on", "only", "or", "other", "our", "own", "rather", "said", "say", "says", "she", "should", "since", "so", "some", "than", "that", "the", "their", "them", "then", "there", "these", "they", "this", "tis", "to", "too", "twas", "us", "wants", "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom", "why", "will", "with", "would", "yet", "you", "your"]
numbers = ["0","1","2","3","4","5","6","7","8","9"]

COMMA_DELIMITER = re.compile("\W+")


def filterWord(word: str):
    if word is not stop_words and word is not numbers:
        return word


if __name__ == "__main__":
    conf = SparkConf().setAppName("task1").setMaster("local[*]")
    sc = SparkContext(conf = conf)


    lines = sc.textFile("hdfs:///user/local/temp/wordcount_data")
    words = lines.flatMap(lambda line: COMMA_DELIMITER.split(line)) \
        .filter(filterWord) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda word: word) \
        .coalesce(1)

    words.saveAsTextFile("hdfs:///tmp/wordcount/spark/task2.1")
