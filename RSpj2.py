'''
Name: YongBaek Cho
Date: 09/06/2018
Homework: ISTA 331 Hw2
Description: This module is used to extract some data from a bookstore
    transaction database and to make recommendations for a customer based
    on their purchase history
'''
import pandas as pd, numpy as np, random, sqlite3


def get_purchase_matrix(conn):
    '''
    Builds a purchase matrix from a bookstore transaction database
    Parameters:
        conn, sqlite3.Connection - a database connection
    Return values:
        purchase_matrix, dict - a dictionary that maps customer id's to
            sorted lists of ISBN's of books they have purchased w/o duplicates
    '''
    cur = conn.cursor()
    query1 = 'SELECT cust_id FROM Customers;'
    query2 = 'SELECT isbn FROM Orders, OrderItems \
            WHERE (Orders.order_num = OrderItems.order_num) and (cust_id = ?)\
            ORDER by isbn;'  #sorted
    purchase_matrix = {}

    for row in cur.execute(query1).fetchall():
        cust_id = row[0]
        purchase_matrix.setdefault(cust_id, [])
        for row2 in cur.execute(query2, (cust_id,)).fetchall():
            isbn = row2[0]
            if isbn not in purchase_matrix.get(cust_id):
                purchase_matrix.get(cust_id).append(isbn)

    return purchase_matrix


def get_empty_count_matrix(conn):
    '''
    Builds a zeroed count matrix from a bookstore transaction database
    Parameters:
        conn, sqlite3.Connection - a database connection
    Return values:
        purchase_matrix, pd.DataFrame - a zeroed dataframe with index and
            columns that are the ISBN's of the books available in the bookstore
    '''
    cur = conn.cursor()
    query = 'SELECT isbn FROM Books \
             ORDER by isbn;'

    isbns = [row[0] for row in cur.execute(query).fetchall()]
    count_matrix = pd.DataFrame(0, index=isbns, columns=isbns)

    return count_matrix


def fill_count_matrix(count_matrix, purchase_matrix):
    '''
    Fills an empty count matrix using a purchase matrix
    Parameters:
        count_matrix, pd.DataFrame - a matrix returned by
            get_empty_count_matrix()
        purchase_matrix, dict - a matrix returned by get_purchase_matrix()
    Return values:
        None
    '''
    for _, isbns in purchase_matrix.items():
        for isbn1 in isbns:
            for isbn2 in isbns:
                count_matrix.loc[isbn1][isbn2] += 1


def make_probability_matrix(count_matrix):
    '''
    Takes a count matrix and returns a conditional probability matrix
    Parameters:
        count_matrix, pd.DataFrame - a full matrix returned by
            fill_count_matrix()
    Return values:
        p_matrix, pd.DataFrame - a zeroed dataframe of probabilities that
            a column book will be purchased  given that the row book
            has been purchased
    '''
    isbns = count_matrix.index
    
    p_matrix = pd.DataFrame(0, index=isbns, columns=isbns)

    for row in isbns:
        for col in isbns:
            prob = -1 if row == col else\
                count_matrix.loc[row, col] / count_matrix.loc[row, row]
            p_matrix.loc[row, col] = prob

    return p_matrix


def sparse_p_matrix(p_matrix):
    '''
    Takes a probability matrix and returns a sparse probability matrix
    Parameters:
        p_matrix, pd.DataFrame - a matrix returned by make_probability_matrix()
    Return values:
        sp_matrix, dict - a dictionary that maps ISBN's to a list of the 15
            ISBN's of the books most likely to be purchased in descending order
            of likelihood
    '''
    isbns = p_matrix.index
    sp_matrix = dict(zip(isbns, [None] * len(isbns)))

    for row in isbns:
        most_likely = sorted((zip(isbns, p_matrix.loc[row])),
                             key=lambda x: x[1])
        most_likely = [x[0] for x in most_likely[-1: -16: -1]]
        sp_matrix[row] = most_likely

    return sp_matrix


def get_cust_id(conn):
    '''
    Displays a list of customers from a bookstore transaction database and
        returns an integer customer id or None, depending upon user input
    Parameters:
        conn, sqlite3.Connection - a database connection
    Return values:
        cust_id, int or None - an integer that has been input by a user
    '''
    cur = conn.cursor()
    query = 'SELECT cust_id, last, first FROM Customers\
        ORDER by cust_id;'

    print('{:5}{:5}{:}'.format('CID', ' ', 'Name'))
    print('{:-^5}{:5}{:-^5}'.format('-', ' ', '-'))
    for row in cur.execute(query).fetchall():
        print('{:>5}{:5}{}'.format(row[0], ' ', row[1] + ', ' + row[2]))
    print('{:-^5}{:-^5}{:-^5}'.format('-', '-', '-'))

    try:
        cust_id = int(input('Enter customer number or enter to quit: '))
    except ValueError:
        cust_id = None

    return cust_id


def purchase_history(cust_id, isbns, conn):
    '''
    Returns a customer's purchase history containing titles
    Parameters:
        cust_id, int - a valid customer id
        isbns - a list of ISBN's that the customer has purchased
        conn, sqlite3.Connection - a database connection
    Return values:
        purchase_hist, str - a string containing the customer's purchase
            history as titles instead of ISBN's
    '''
    cur = conn.cursor()
    query1 = 'SELECT isbn, book_title FROM Books;'
    query2 = 'SELECT last, first FROM Customers\
            WHERE (cust_id = ?);'

    book_titles = {row[0]: row[1] for row in cur.execute(query1).fetchall()}
    cust_name = ' '.join(cur.execute(query2, (cust_id,)).fetchone()[::-1])

    first_line = ('Purchase history for ' + cust_name)[:80]
    purchase_hist = first_line + '\n'
    purchase_hist += '-' * len(first_line) + '\n'
    for isbn in isbns:
        purchase_hist += book_titles[isbn][:80] + '\n'
    purchase_hist += '-' * 40 + '\n'

    return purchase_hist


def get_recent(cust_id, conn):
    '''
    Gets a random ISBN from a customer's most recent order in a bookstore
        transaction database
    Parameters:
        cust_id, int - a valid customer id
        conn, sqlite3.Connection - a database connection
    Return values:
        rr_isbn, str - a ISBN chosen randomly from the customer's most recent
            order
    '''
    cur = conn.cursor()
    query1 = 'SELECT order_num FROM Orders\
             WHERE (cust_id = ?)\
             ORDER by order_date DESC;'
    query2 = 'SELECT isbn FROM OrderItems\
            WHERE (order_num = ?);'

    last_order = cur.execute(query1, (cust_id,)).fetchone()[0]
    isbns = [row[0] for row in cur.execute(query2, (last_order,)).fetchall()]
    rr_isbn = isbns[random.randrange(len(isbns))]

    return rr_isbn


def get_recommendation(cust_id, sp_matrix, isbns, conn):
    '''
    Makes a recommendation for a customer
    Parameters:
        cust_id, int - a valid customer id
        sp_matrix, dict - a matrix returned by sparse_p_matrix()
        isbns, list - a list of ISBN's that the customer has purchased
        conn, sqlite3.Connection - a database connection
    Return values:
        rr_isbn, str - a string containing at most 2 new books that can be
            advertised to the customer
    '''
    cur = conn.cursor()
    query1 = 'SELECT last, first FROM Customers\
            WHERE (cust_id = ?);'
    query2 = 'SELECT book_title FROM Books\
            WHERE (isbn = ?);'

    rr_isbn = get_recent(cust_id, conn)
    cust_name = ' '.join(cur.execute(query1, (cust_id,)).fetchone()[::-1])
    new_isbns = [x for x in sp_matrix[rr_isbn] if x not in isbns][:2]
    book_titles = [cur.execute(query2, (x,)).fetchone()[0] for x in new_isbns]

    first_line = ('Recommendations for ' + cust_name)[:80]
    recomm = first_line + '\n'
    recomm += '-' * len(first_line) + '\n'
    for book_title in book_titles:
        recomm += book_title + '\n'
    if not book_titles:
        recomm += 'Out of ideas, go to Amazon' + '\n'

    return recomm
