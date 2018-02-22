# Project Goal
Implement a watered-down version of the PageRank algorithm, using parsed data from a sample of Wikipedia's edit history.
## Authors
Elizabeth Konstantin Kwek Jin Li \\ 2287563K
Ng Kai Feng Bernard \\ 2287600N
University of Glasgow Computing Science Level 4

# Usage
Choose where to access data files and execute jobs using the following:
```bash
sudo select-cluster [local | idup | bd]
```
Then from within this directory, run the following commands on the terminal:
```bash
mvn clean package
export HADOOP_CLASSPATH="$PWD/target/uog-bigdata-0.0.1-SNAPSHOT.jar"
hadoop ax.PageRank [input path] [output folder name] [rounds]
```
# Implementation
The process is split into **two** main jobs, `Init` and `Iter`. `Init` extracts the relevant data and pairs each outbound link with the calling article. `Iter` then takes these Key-Value pairs and calculates the page rank of every mentioned article. This is done in as many iterations as specified by the integer `[rounds]` in the command:
```bash
hadoop ax.PageRank [input path] [output folder name] [rounds]
```
If `[rounds]` is not specified, the default `2` is used.

## Sample Data (Abridged)
```
REVISION 12 30364918 Anarchism 2005-12-06T17:44:47Z RJII 141644
CATEGORY Social_philosophy Political_theories Forms_of_government Anarchism Political_ideology_entry_points
IMAGE Zerzan.jpeg Goldman-4.jpg Kropotkin.jpg BenjaminTucker.jpg Blkred_flag.png Bakuninfull.jpg JohannMost.jpg Murray_Rothbard_Smile.JPG LeoTolstoy.jpg Proudhon-young.jpg
MAIN 467_BC Seattle,_Washington Edward_Abbey Félix_Guattari Head_of_state John_Brown_(abolitionist) Environmental_movement Auroville Peter_Kropotkin Peter_Kropotkin Peter_Kropotkin Peter_Kropotkin Public_key_cryptography Autonomism
TALK
USER
USER_TALK
OTHER Eu:Anarkismo Ar:لاسلطوية No:Anarkisme Sl:Anarhizem Is:Stjórnleysisstefna Fa:دولت‌زدائی Lt:Anarchizmas Gl:Anarquismo
EXTERNAL http://dwardmac.pitzer.edu/Anarchist_Archives/bright/berkman/comanarchism/whatis_toc.html http://www.retailworkers.org/ http://www.infoshop.org/inews/article.php?story=05/01/06/7640998 http://www.barefootsworld.net/nockoets0.html
TEMPLATE Mergefrom Anarchism
COMMENT /* The first self-labeled anarchist */ small clarification on expropriation
MINOR 0
TEXTDATA 11530
```
## 1) Data Initialization
Of interest to this project are the lines `REVISION` and `MAIN`.
In `REVISION`, the third item `article_title` is used to uniquely identify relevant articles, and is used as Keys in the [InitMapper](/src/main/java/ax/InitMapper.java) during initialization. **All revisions are considered in this implementation.**   
`MAIN` provides the `article_title`s of other pages linked to (i.e., ***outlinks***) by the current article. All references are mapped and passed on to [InitReducer](/src/main/java/ax/InitReducer.java), which then **filters out repeated data**, and assigns each article an initial PageRank score (more on this [below](#2-ranking-rounds)).
### Input / Output
[InitMapper.java](/src/main/java/ax/InitMapper.java)
>Input:   
> K: [Line Number]   
> V: [Revision Record - 14lines, including 1 empty line at the end]   
>   
>Output:   
> K: `article_title`   
> V: `outlink_title`   
>Output: (if no data in MAIN)   
> K: `article_title`   
> V: empty   

[InitReducer.java](/src/main/java/ax/InitReducer.java)
>Input:   
> K: `article_title`   
> V: `outlink_title` (or empty)   
>   
>Output:   
> K: `article_title`   
> V: `PR    outlink1,outlink2,....`   

## 2) Ranking Rounds
### PageRank Formula
```
PR(u)= (1 - DF) + DF * Sum(PR(v)/L(v))
```
`PR(v)/L(v)` represents the contribution of an article `v` that links to the page `u`. The contributions of ALL pages that reference to `u` are totalled and multiplied by the Damping Factor (`DF`), which in this project is set to `0.85`. The result is added to `1 - DF` to get the PageRank of `u`.
This formula is applied in rounds, with all pages initially assigned a PageRank score of `1.0` (`INIT_RANK`).
The values of `INIT_RANK` and `DF` are set in the main [PageRank](/src/main/java/ax/PageRank.java) class and sent to the `Init` and `Iter` reducers respectively to be used in the calculation of each page's the PageRank score.
### Iterations
To run this step in repeated rounds, the [IterMapper](/src/main/java/ax/IterMapper.java) produces two kind of output:
> A - preserves the list of outlinks from each page   
> B - sends the relevant numbers needed for calculating PageRank   

The [IterReducer](/src/main/java/ax/IterMapper.java) then uses `Output B` to calculate the PageRank score of an article, and outputs that score in the same format as the input of the Mapper.
After each step, the main PageRank class reassigns the input and output paths and starts a new Job running the same task.
### Input / Output
[IterMapper.java](/src/main/java/ax/IterMapper.java)
>Input:   
>> K: [Line Number]   
> V: `article_title    PR    outlink1,outlink2,....`   
>   
>Output A:   
> K: `article_title`   
> V: `LINK_PREFIX    outlink1,outlink2,....`   
>Output B:   
> K: `linkX`   
> V: `PR    count(outlinks from title)`   

Notes: `LINK_PREFIX` is an arbitrary string to help IterReducer differentiate the 2 output types. `linkX` refers to an outlink from `article_title`. Every outlink will have its own Key-Value pair.  

[IterReducer.java](/src/main/java/ax/IterReducer.java)
>Input A:   
> K: `article_title`   
> V: `LINK_PREFIX    outlink1,outlink2,....`   
>Input B:   
> K: `linkX`   
> V: `PR`    count(outlinks from title)   
>   
>Output:   
> K: `article_title`   
> V: `PR    outlink1,outlink2,....`   
>Output: (LAST ITERATION)   
> K: `article_title`   
> V: `PR`   

# Modifications to Provided Files
None. Only the package name in the `hadoop` command is changed, from `mapreduce` to `ax`. The files in the `mapreduce` directory are removed.
