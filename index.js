const express = require("express");
const app = express();
const cors = require("cors");
const pool = require("./db");

// middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({extended:true}));

app.get('/2.1.1', async (req,res) =>{
    try {
        const sql ='Select count(*) from film';
        const query_res = await pool.query(sql);
        res.json(query_res.rows);


    } catch (error) {
        console.error(error);
        res.status(404);
        req.json({message: error.message});
    }
});

app.get('/2.1.2', async (req, res) => {
    try {
        const sql = `
       Select rating as "rating",
       sum(length)
       from film
       group by rating
       order by rating asc
        `;

        const query_res = await pool.query(sql);
        res.json(query_res.rows);
    } catch (error) {
        console.error(error);
        res.status(404);
        res.json({ message: error.message });
    }
});

app.get('/2.1.3', async (req, res) => {
    try {
        const sql = `
        SELECT rating, sum(rental_duration)
        FROM film
        GROUP by rating 
        ORDER by rating asc
            `;

        const query_res = await pool.query(sql);
        res.json(query_res.rows);
    } catch (error) {
        console.error(error);
        res.status(404);
        res.json({ message: error.message });
    }
});


app.get('/2.2.1', async (req, res) => {
    try {
        const sql = `
        select * FROM midterm_list_of_films
            `;

        const query_res = await pool.query(sql);
        res.json(query_res.rows);
    } catch (error) {
        console.error(error);
        res.status(404);
        res.json({ message: error.message });
    }
});

app.get('/2.2.2', async (req, res) => {
    try {
        const sql = `
        select  * from  midterm_total_films_per_category
            `;

        const query_res = await pool.query(sql);
        res.json(query_res.rows);
    } catch (error) {
        console.error(error);
        res.status(404);
        res.json({ message: error.message });
    }
});

app.get('/2.3.1', async (req, res) => {
    try {
        const sql = `
        SELECT f.title, c.name, f.rental_duration, NTILE(4) OVER (ORDER BY f.rental_duration) AS standard_quartile
        FROM film_category fc
        JOIN category c
        ON c.category_id = fc.category_id
        JOIN film f
        ON f.film_id = fc.film_id
        WHERE c.name IN ('Animation', 'Children', 'Classics', 'Comedy', 'Family', 'Music')
        ORDER BY 4
            `;

        const query_res = await pool.query(sql);
        res.json(query_res.rows);
    } catch (error) {
        console.error(error);
        res.status(404);
        res.json({ message: error.message });
    }
});

app.get('/2.3.2', async (req, res) => {
    try {
        const sql = `
        SELECT f.title, c.name, COUNT(r.rental_id)
        FROM film_category fc
        JOIN category c
        ON c.category_id = fc.category_id
        JOIN film f
        ON f.film_id = fc.film_id
        JOIN inventory i
        ON i.film_id = f.film_id
        JOIN rental r 
        ON r.inventory_id = i.inventory_id
        WHERE c.name IN ('Animation', 'Children', 'Classics', 'Comedy', 'Family', 'Music')
        GROUP BY 1, 2
        ORDER BY 2, 1
            `;

        const query_res = await pool.query(sql);
        res.json(query_res.rows);
    } catch (error) {
        console.error(error);
        res.status(404);
        res.json({ message: error.message });
    }
});

app.post('/2.4.1/:id', async (req, res) => {
    try {
        const {id} = req.params;
        const sql = ' select findstaff($1);';
        const out = await pool.query(sql,[id]);
        res.json(out.rows);
    } catch (error) {
        console.error(error);
        res.status(404);
        res.json({ message: error.message });
    }
});


app.post('/2.4.2', async (req, res) => {
    try {
        const data = req.body;
        const query = await pool.query('call rental(r_id => $1,s_id => $2);',[data.r_id,data.s_id]);
        res.json(query.rows);
    } catch (error) {
        console.error(error);
        res.status(404);
        res.json({ message: error.message });
    }
});


app.listen(5000, () => {
    console.log("server has started on port localhost:5000");
});

