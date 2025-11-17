const cron = require('node-cron');
const axios = require('axios');
const pushFile = require('./dataLakeConn');
require('dotenv').config();


async function searchMovie(title) {
    const API_KEY = process.env.TMDB_API_KEY;
    let res;
    try {
        const response = await axios.get(`https://api.themoviedb.org/3/search/multi`, {
            params: {
                api_key: API_KEY,
                query: title,
                language: 'sk-SK' // Slovak language
            }
        });

        if (response.data.results.length > 0) {
            res = response.data.results[0]; // Return first result
        } else {
            return null;
        }
    } catch (error) {
        console.error('Search error:', error.message);
        return null;
    }

    try {

        if (res.media_type === 'movie') {
            const movieData = await axios.get(`https://api.themoviedb.org/3/movie/${res.id}?append_to_response=credits&language=en-US&api_key=${API_KEY}`);
            return {...movieData.data, media_type: res.media_type};
        }
        else if (res.media_type === 'tv') {
            const tvData = await axios.get(`https://api.themoviedb.org/3/tv/${res.id}?append_to_response=credits&language=en-US&api_key=${API_KEY}`);
            return {...tvData.data, media_type: res.media_type};
        }

    } catch(error) {
        console.error('Search error: ', error.message)
        return null
    }
}

searchMovie('teoria velkeho tresku').then(res => console.log(res));

