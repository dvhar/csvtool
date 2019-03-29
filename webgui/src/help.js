import React from 'react';

export class Help extends React.Component {
    render(){
        if (!this.props.show) return <></>
        return ( 
            <div className="helpBox">
            <h3>What this software does</h3>
            <hr/>
            Query data from csv files, display it, and save it to new csv files. Unlike many other programs that do the same thing, this one won't eat up all your computer's resources when querying huge multi-gigabyte files.
            <br/>
            The program will show you the first 1000 results in the browser, with 2 options for viewing certain rows or columns. The result table header includes the column number and data type. You can click on a column header to sort the top 1000 results by that column.
            <h3>How to save files</h3>
            <hr/>
            After running a query, hit the save button. All the queries on the page will be run again, but this time they will be saved to csv files. If there are multiple queries on the page, you still only need to specify one file and a number will be added to the filename for each one. For file-size reasons, the 'order by' option does not currently effect output files, only the results that are displayed in the browser.
            <h3>How to use the query language</h3>
            <hr/>
            A new SQL interpretter had to be written to efficiently handle huge csv files and not all SQL standards have been implemented. It is based on TSQL, with some features missing and some conveniences added. This section describes this dialect of SQL. Keywords are case-insensitive, column names and values are not.
            <blockquote>
                <h4>Specifying a file</h4>
                Query files the same you you would query tables, using the 'from' keyword.
                <h4>Selecting some columns</h4>
                Columns can be specified by name or number. If some columns have the same name, the later ones must be specified by number. Commas are optional, so you can easily copy and paste column names or numbers from a result table header into a new query.
                <br/><br/>
                Example: selecting columns 1-3, dogs, and cats from a file<br/>
                <blockquote>
                    select 1 2 3 dogs cats from /home/user/pets.csv
                </blockquote>
                <h4>Selecting all columns</h4>
                'select * ...' works how you'd expect. If you don't specify any columns at all, it will also select all. 
                <br/><br/>
                Examples of selecting all columns:
                <br/>
                <blockquote>
                select * from /home/user/pets.csv
                <br/>
                select from /home/user/pets.csv
                </blockquote>
                <h4>selecting rows with a distinct value</h4>
                Use the 'distinct' keyword in front of the column. If selecting all, use 'distinct' and '*'.
                <br/><br/>
                Examples:
                <blockquote>
                    select distinct 3 from /home/user/pets.csv
                    <br/>
                    select 1 2 3 distinct dogs cats from /home/user/pets.csv
                    <br/>
                    select distinct dogs * from /home/user/pets.csv
                </blockquote>
                <h4>Selecting a number of rows</h4>
                Use the 'top' keyword, just like TSQL. Be careful not to confuse the number after 'top' for a column number.
                <br/><br/>
                Example: selecting columns 1-3, dogs, and cats from a file, but only 100 results<br/>
                <blockquote>
                    select top 100 1 2 3 dogs cats from /home/user/pets.csv
                </blockquote>
                <h4>Selecting rows that match a certain condition</h4>
                Use the 'where' keyword. Columns can be specified by name or number, though be careful not to confuse column numbers for comparision values like in the third example below. Use any combinatin of 'column [relational operator] value', parentheses, 'and', 'or', 'not', and 'between'. Put quotation marks ( ' and " both work) around anything that contains spaces. Dates are handled nicely, so 'May 18 1955' is the same as 5/18/1955.
                <br/><br/>
                Valid relational operators are {'=,  !=,  <>,  >,  <,  >=,  <=,  and '}'between'. '!' is evaluated the same as 'not', and can be put in front of a relational operator or a whole comparison.
                <br/><br/>
                Examples:
                <blockquote>
                select from /home/user/pets.csv where name = scruffy or name = "fuzzy wuzzy"
                <br/>
                select from /home/user/pets.csv where dateOfBirth {'<'} 'april 10 1980' or age between 20 and 30
                <br/>
                select from /home/user/pets.csv where {'(1 < 13 or fuzzy = very) and not (3 = null or weight >= 50)'}
                </blockquote>
            </blockquote>
            </div>
        )
    }
}
