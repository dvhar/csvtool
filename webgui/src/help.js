import React from 'react';

export class Help extends React.Component {
	render(){
		if (!this.props.show) return <></>
		return ( 
			<div className="helpBox">
			<h3>What this software does</h3>
			<hr/>
			{"Run queries on csv files, display the results, and save to new csv files. It can handle big csv files without eating  up all you computer's resources."}
			<br/>
			{"The program will show you the first several hundred results in the browser, with 2 options for viewing certain rows or columns. You can click on a column header to sort the displayed results by that column."}
			<h3>How to save files</h3>
			<hr/>
			{"After running a query, hit the save button. Navigate to where you want to save, type in the file name, and hit the save button to the right. All the queries on the page will run again, but this time they will be saved to csv files. If there are multiple queries on the page, you still only need to specify one file and a number will be added to the filename for each one."}
			<h3>How to use the query language</h3>
			<hr/>
			{"This program uses a custom version of SQL based on TSQL. Some features like Unions and Subqueries are not implemented yet. The query is ordered like this:"}
			<br/>
			{"<selections> <from file> <joins> <where> <group by> <having> <order by>"}
			<br/>
			<blockquote>
				<h4>Specifying a file</h4>
					{"Click 'Browse Files' to find files, and double click one to add it to the query. You can query different files at the same time by separating queries with a semicolon. If a file has no header, add 'nh' or 'noheader' after the file path or alias, or before 'select'."}
					<h4>Selecting some columns</h4>
					{"Columns can be specified by name or number. Select column numbers by prefacing the number with 'c', or by using a plain unquoted number if putting a 'c' in front of the whole query. Commas between selections are optional."}
					<br/><br/>
					Example: selecting columns 1-3, dogs, and cats from a file<br/>
					<blockquote>
						{"select c1 c2 c3 dogs cats from '/home/user/pets.csv'"}
						<br/>
						{"select c1 c2 c3 dogs cats from 'C:\\users\\dave\\pets.csv' nh"}
						<br/>
						{"c select 1 2 3 dogs cats from 'C:\\users\\dave\\pets.csv'"}
					</blockquote>
				<h4>Selecting all columns</h4>
					{"'select * ' works how you'd expect. If you don't specify any columns at all, it will also select all. "}
					<br/><br/>
					Examples:
					<br/>
					<blockquote>
						{"select * from '/home/user/pets.csv'"}
						<br/>
						{"select from '/home/user/pets.csv'"}
					</blockquote>
				<h4>Selecting more complex expressions</h4>
					{"Use + - * / % operators to add, subtract, multiply, divide, and modulus expressions. You can combine them with parentheses. You can also use case expressions."}
					<br/><br/>
					Examples:
					<blockquote>
						{"select birthdate+'3 weeks', c1*c2, c1/c2, c1-c2, c1%2, (c1-23)*(c2+34) from '/home/user/pets.csv'"}
						<br/>
						{"select case when c1*2<10 then dog when c1*10>=10.2 then cat else monkey end from '/home/user/pets.csv'"}
						<br/>
						{"select case c1 / c4 when (c3+c3)*2 then dog when c1*10 then cat end from '/home/user/pets.csv'"}
					</blockquote>
				<h4>Non-aggregate functions</h4>
					{"These functions can be used to get certain pieces of information from a date value:"}
					<blockquote>
						year month monthname week day dayname dayofyear dayofmonth dayofweek hour
					</blockquote>
					{"The 'abs' function can be used to get the absolute value of a number."}
					<br/>
					{"The 'inc' function returns an automatically incrementing number. Leave the parentheses empty like inc() to increment by 1, or include a number to increment by that number like inc(2)."}
					<br/>
					<br/>
					{"encrypt() and decrypt() functions encrypt and decrypt an expression. Can be used with 1, 2, or 3 parameters. encrypt(expression), encrypt(expression, cipher), and encrypt(expression, cipher, password) are all valid. Cipher can be 'aes' if you want strong encryption, or 'rc4' if you want something a government could crack but takes less space. Default behavior uses the aes cipher and prompts you for a password."}
				<h4>Aggregate functions</h4>
					{"These functions are used to do calculations with multiple rows. They can be used with a 'group by' clause, which goes after the 'from' clause and before the 'order by' clause. Without a 'group by' clause, all rows will be calculated together into a single result row. You can group by multiple expressions. You can filter with a 'having' clause."}
					<blockquote>
						sum  avg  min  max  count stdev stdevp
					</blockquote>
					Example of functions:
					<blockquote>
						{"select count(*), sum('visit duration'), avg(revenue)-avg(cost) as profit, monthname(date), week(date) from '/home/user/pets.csv' group by month(date) week(date) order by avg(cost)"}
					</blockquote>
				<h4>Selecting rows with a distinct value</h4>
					{"Put the 'distinct' keyword in front of the expression that you want to be distinct. Put 'hidden' after 'distinct' if you don't want that value to show up in the results."}
					<br/><br/>
					Examples:
					<blockquote>
						{"select distinct c3*c3 from '/home/user/pets.csv'"}
						<br/>
						{"select c1 c2 c3 distinct dogs cats from '/home/user/pets.csv'"}
						<br/>
						{"select distinct hidden dogs * from '/home/user/pets.csv'"}
					</blockquote>
				<h4>Selecting a number of rows</h4>
					{"Use the 'top' keyword after 'select'. Be careful not to confuse the number after 'top' for part of an expression."}
					<br/><br/>
					Examples: selecting columns 1-3, dogs, and cats from a file, but only fetch 100 results.<br/>
					<blockquote>
						{"select top 100 c1 c2 c3 dogs cats from '/home/user/pets.csv'"}
					</blockquote>
				<h4>Selecting rows that match a certain condition</h4>
					 {"Use any combinatin of '[expression] [relational operator] [expression]', parentheses, 'and', 'or', 'not', and 'between'. Dates are handled nicely, so 'May 18 1955' is the same as '5/18/1955'. Empty entries evaluate to 'null'."}
					<br/><br/>
					{"Valid relational operators are =,  !=,  <>,  >,  <,  >=,  <=, like, and between. '!' is evaluated the same as 'not', and can be put in front of a relational operator or a whole comparison."}
					<br/><br/>
					Examples:
					<blockquote>
						{"select from '/home/user/pets.csv' where dateOfBirth < 'april 10 1980' or age between (c3-19)*1.2 and 30"}
						<br/>
						{"select from '/home/user/pets.csv' where (c1 < c13 or fuzzy = very) and not (c3 = null or weight >= 50 or name not like 'a_b%')"}
						<br/>
						{"select from '/home/user/pets.csv' where c1 in (2,3,5,7,11,13,17,19,23)"}
					</blockquote>
				<h4>Sorting results</h4>
					{"Use 'order by' at the end of the query, followed by one column or expression, followed optionally by 'asc'. Sorts by descending values unless 'asc' is specified."}
					<br/><br/>
					Examples:
					<blockquote>
						{"select from '/home/user/pets.csv' where dog = husky order by age asc"}
						<br/>
						{"select from '/home/user/pets.csv' order by c2*c3"}
					</blockquote>
				<h4>Joining Files</h4>
					{"Any number of files can be joined together. Left and inner joins are allowed, default is inner. One join condition per join file. Each file needs an alias. Joins with many files can be sorted (with order-by clause) if doing aggregate functions, but only joins with 2 files can be sorted if not using aggregates."}
					<br/><br/>
					Examples:
					<blockquote>
						{"select pet.name, pet.species, code.c1 from '/home/user/pets.csv' pet"
							+" left join '/home/user/codes.csv' code"
							+" on pet.species = code.species order by pet.age"}
						<br/>
						{"select pet.name, code.c1, fur.flufftype from '/home/user/pets.csv' pet"
							+" inner join '/home/user/fur.csv' fur"
							+" on pet.fluffiness = fur.fluff"
							+" left join '/home/user/codes.csv' code"
							+" on pet.species = code.species"}
					</blockquote>
			</blockquote>
			<h3>Ending queries early, viewing older queries, and exiting</h3>
			<hr/>
			{"If a query is taking too long, hit the button next to submit and the query will end and display the results that it found."}
			<br/>
			{"The browser remembers previous queries. In the top-right corner, it will show you which query you are on. You can re-run other queries by hitting the forward and back arrows around the numbers."}
			<br/>
			{"To exit the program, just leave the browser page. The program exits if it goes 2 minutes without being viewed in a browser."}
			<h3>Waiting for results to load</h3>
			<hr/>
			{"Browsers can take a while to load big results, even when limiting the number of rows. If the results of a query look similar to the results of the previous query, you can confirm that they are new by checkng the query number in between the forward/back arrows in the top-right corner."}
			<br/><br/>
			<hr/>
			version 0.73 - 7/1/2020
			<hr/>
			<br/><br/>
			</div>
		)
	}
}
