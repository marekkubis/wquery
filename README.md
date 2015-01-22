About
-----

WQuery is a suite of tools for processing WordNet-like lexical databases.
The suite consists of a family of domain-specific languages for querying
and transforming wordnets and a set of shell commands that ease processing of
lexical databases in the unix environment.

[![Build Status](https://travis-ci.org/marekkubis/wquery.svg?branch=master)](https://travis-ci.org/marekkubis/wquery)

Installation
------------

WQuery requires Java Runtime Environment (JRE) version 1.6 or higher to run.
After downloading and unpacking the tools are ready to use.
It is also useful to add the `bin` subdirectory of WQuery to `$PATH`.

Getting Started
---------------

All the tools in the suite share a common binary representation of a WordNet-like lexical database.
Hence, the first step is to convert the database to the WQuery format.
This can be done using the `wcompile` command.

    # wcompile samples/samplenet.xml > sample.wq

Having the wordnet in the `wq` format, one can use the WPath language to inspect its contents.
For instance, to search for noun synsets that do not have hypernyms one can pass the query
`{}[pos = `n` and empty(hypernym)]` after the `-c` option of the `wpath` command.

    # wpath -c '{}[pos = `n` and empty(hypernym)]' sample.wq

    { entity:1:n }
    { people:1:n }
    { organism:1:n being:2:n }
    ...

Beside querying, one can also transform wordnets using the WUpdate language. For example, to remove
cross part-of-speech hypernymy links from `sample.wq` and store the updated wordnet in `sample2.wq`
one can invoke the following command

    # wupdate -c 'from {}$a.hypernym$b[$a.pos != $b.pos] update $a hypernym -= $b' sample.wq > sample2.wq

The updated database may be converted to alternative wordnet representation formats using the `wprint` command.
For instance, to save the wordnet in the [LMF](http://www.lexicalmarkupframework.org/) format one can execute the following command

    # wprint -t lmf sample2.wq > sample2.xml

Finally, in order to omit intermediate results the commands may be combined using pipes

    # wcompile samples/samplenet.xml | wupdate -c 'from {}$a.hypernym$b[$a.pos != $b.pos] update $a hypernym -= $b' | wprint -t lmf > sample2.xml

Citing
------

If you have found WQuery useful, please consider citing the following paper

Marek Kubis. A Query Language for WordNet-like Lexical Databases. In Jeng-Shyang Pan, Shyi-Ming Chen, and Ngoc-Thanh Nguyen, editors, Intelligent Information and Database Systems, volume 7198 of Lecture Notes in Artificial Intelligence, pages 436–445. Springer Heidelberg, 2012.

If you use update facilities of the system, you may also consider citing the WUpdate paper

Marek Kubis. A Tool for Transforming WordNet-Like Databases. In Zygmunt Vetulani and Joseph Mariani, editors, Human Language Technology Challenges for Computer Science and Linguistics, volume 8387 of Lecture Notes in Computer Science, pages 343–355. Springer International Publishing, 2014.

For the purpose of compiling [Princeton WordNet](http://wordnet.princeton.edu) `wcompile -t pwn` command uses internally the [JWI](http://projects.csail.mit.edu/jwi/) library.

The reference for citing JWI can be found at the JWI author's [website](http://projects.csail.mit.edu/jwi/).

License Terms
-------------

This software is provided under the terms of a BSD-like license (see [LICENSE](LICENSE)).

The licenses of third-party libraries that are distributed with WQuery can be found in the [doc/licenses](src/universal/doc/licenses) directory.

Further Information
-------------------

The syntax of the query languages available in WQuery is discussed in the [User Guide](https://github.com/marekkubis/wquery/wiki).

Examples of queries of practical importance can be found in the [test suite](https://github.com/marekkubis/wquery/blob/master/src/test/scala/org/wquery/FunctionalTestSuite.scala).
