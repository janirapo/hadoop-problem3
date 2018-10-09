# hadoop-problem3

These programs are made for the University of Helsiki course Big Data Management 2018, problem 3.

The assignment goes as follows:
```
Hadoop can also be used for approximate string processing. Implement Hadoop MapReduce
programs to perform the similarity string join with n-gram Jaccard similarity (https://www.joyofdata.de/blog/comparison-of-string-distance-algorithms/).
Download datasets: https://www.cs.helsinki.fi/u/jilu/dataset/Gram2018.zip, where you will find
two Wikipedia samples. Each of them contains 10k lines of Wikipedia categories like https://en.wikipedia.org/wiki/Category:Power_stations. These
datasets are now inconsistent due to misspellings (https://en.wikipedia.org/wiki/Wikipedia:Lists_of_common_misspellings).
Can you find similar pairs of records, one from each dataset, which have Jaccard distance > 0 and <= 0.15​?

Fill the following table: (20 points)
| Gram size | Result size | Running time on a single VM (s) | Running time on a multiple VMs (s), specify how many VMs |
| --------- | ----------- | ------------------------------- | -------------------------------------------------------- |
| 2 (bi-gram) | --------- | ------------------------------- | -------------------------------------------------------- |
| 3 (tri-gram) | -------- | ------------------------------- | -------------------------------------------------------- |

2. Describe your algorithm for this problem, and upload your source codes and analyze the
performance of your codes. (20 points)

Hint 1: You may need to use two MapReduce procedures: first find potential pairs and then
verify them (i.e., “filtering and verification”). Ref: http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.168.5695&rep=rep1&type=pdf.
Hint 2: You may feed the output of one Reducer directly to another Mapper by using the
following lines:
job1.setOutputFormatClass(SequenceFileOutputFormat.class)
and
job2.setInputFormatClass(SequenceFileInputFormat.class)
```
