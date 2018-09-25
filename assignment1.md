# Assginment 1 Answers

# Question 1
Pairs: There are 2 MapReduce jobs. The first job is using to count the single word appearance(N(A)) and the total lines 
of the input file. The second job is using to count the appearance of the co-occurring pairs and calculate the PMI values
of the pairs. Both jobs use the input file as the input, however, the output of the first job is going to folder "firstjoboutput" 
and being used as the intermediate data in the second job(setup in the Reducer to retrieve total counts of single word 
and total lines to calculate PMI). The output of the second job is key(A,B) with its value pair(PMI,count), and then put it into output file.

Stripes: This class is working similar to Pairs, 2 MapReduce jobs in total. The difference is that in the second job, it
used single word A as the key, and HMapStIW as the value in the Mapper. The occurring words B of A is put into the HMapStIW
and signed a value of one. The we sum up the values of Bs in the Reducer. The final output of the second job is the key(A)
with its value pair(B,(PMI,count)).

# Question 2
Pairs finishes in 4.904 + 28.475 = 33.379 \
Stripes finishes in 3.86 + 15.482 = 19.342 \
Run on linux.student.cs.uwaterloo.ca

# Question 3
Pairs finishes in 4.966 + 28.489 = 33.455 \
Stripes finishes in 3.833 + 16.439 = 20.272 \
Run on linux.student.cs.uwaterloo.ca

# Question 4.
  77198  308792 2327661


# Question 5. (highest PMI)
| pair | (pim,count) |
| ------ | ------ |
| (maine, anjou) | (3.6331422, 12) |
| (anjou, maine) | (3.6331422, 12) |


# Question 5. (lowest PMI)
| pair | (pim,count) |
| ------ | ------ |
| (thy, you) | (-1.5303967, 11) |
| (you, thy)   |   (-1.5303967, 11) |

This means that given one of "maine" or "anjou", there is high probability to see the other word(They appear on the same
line frequently). \
The same for pair(thy, you).

# Question 6. ('tears')
| pair | (pim,count) |
| ------ | ------ |
|(tears, shed)|   (2.1117902, 15)
|(tears, salt) |  (2.0528123, 11)
|(tears, eyes)  | (1.165167, 23)


# Question 6. ('death')
| pair | (pim,count) |
| ------ | ------ |
|(death, father's)  |     (1.120252, 21)
|(death, die)  |  (0.75415933, 18)
|(death, life)  | (0.7381346, 31)

# Question 7.
| pair | (pim,count) |
| ------ | ------ |
|(hockey, defenceman)  |  (2.4177904, 153)
|(hockey, winger)      |  (2.3697948, 188)
|(hockey, sledge)      |  (2.348635, 93)
|(hockey, goaltender)  |  (2.2534416, 199)
|(hockey, ice) |  (2.2082422, 2160)


# Question 8.
| pair | (pim,count) |
| ------ | ------ |
|(data, cooling)| (2.0971367, 74)
|(data, encryption)|      (2.043605, 53)
|(data, array) |  (1.9901569, 50)
|(data, storage)| (1.9870713, 110)
|(data, database)|        (1.8871822, 99)