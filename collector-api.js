const cron = require('node-cron');
const axios = require('axios');
const pushFile = require('./dataLakeConn');
require('dotenv').config();

const TV_SOURCES = [
    {
        baseUrl: 'http://91.99.234.80:5000',
        stations: [
            { id: 'hbo', endpoint: '/streaming?station=hbo', type: 'program' },
            { id: 'joj-cinema', endpoint: '/streaming?station=joj-cinema', type: 'program' },
            { id: 'film-plus', endpoint: '/streaming?station=film-plus', type: 'program' }
        ]
    },
    {
        baseUrl: 'https://merry-briefly-dinosaur.ngrok-free.app',
        stations: [
            { id: 'hbo3', endpoint: '/hbo_3/program', type: 'program' },
            { id: 'cinemax', endpoint: '/cinemax/program', type: 'program' },
            { id: 'cinemax2', endpoint: '/cinemax_2/program', type: 'program' }
        ]
    },
    {
        baseUrl: 'http://37.9.171.199:8000',
        stations: [
            { id: 'hbo-2', endpoint: '/now?channel=hbo-2', type: 'program' },
            { id: 'nova-cinema', endpoint: '/now?channel=nova-cinema', type: 'program' },
            { id: 'filmbox', endpoint: '/now?channel=filmbox', type: 'program' }
        ]
    },
    {
        baseUrl: 'http://dgx.uvt.tuke.sk:5001',
        stations: [
            { id: 'discovery-bbc-natgeo', endpoint: '/now-playing', type: 'program' },
        ]
    },
    {
        baseUrl: 'http://3.79.27.222:3000',
        stations: [
            { id: 'dajto', endpoint: '/dajto/pull', type: 'program' },
            { id: 'prima-sk', endpoint: '/prima-sk/pull', type: 'program' },
            { id: 'markiza-krimi', endpoint: '/markiza-krimi/pull', type: 'program' }
        ]
    },
];

// TMDB search
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

// Generic function to fetch TV program data from any source
async function fetchTVProgramData(source, station) {
    try {
        const url = `${source.baseUrl}${station.endpoint}`;
        console.log(`Fetching TV data from: ${url}`);

        const response = await axios.get(url, {
            timeout: 10000,
        });

        const data = typeof response.data === 'string' ? JSON.parse(response.data) : response.data;

        return {
            ...data,
            source: source.name,
            station_id: station.id,
            collected_at: new Date().toISOString()
        };
    } catch (error) {
        console.error(`Error fetching data from ${source.name}/${station.id}:`, error.message);
        return null;
    }
}

// Function to collect and store data from all TV sources
async function collectAndStoreTVData() {
    console.log(`[${new Date().toISOString()}] Starting TV data collection from all sources...`);

    let totalCollected = 0;

    // Process each TV source
    for (const source of TV_SOURCES) {
        console.log(`\nProcessing source: ${source.name}`);

        // Process each station in the source
        for (const station of source.stations) {
            try {
                const programData = await fetchTVProgramData(source, station);

                if (programData && !Array.isArray(programData)) {

                    // fetch additional TV channel data
                    let filmData;
                    if (programData.title) {
                        filmData = await searchMovie(programData.title);
                    } else if (programData.items[station.id][0].title) {
                        filmData = await searchMovie(programData.items[station.id][0].title);
                    }

                    
                    const infoToAppend = filmData ? {
                        adult: filmData.adult,
                        original_language: filmData.original_language,
                        original_title: filmData.original_title,
                        popularity: filmData.popularity,
                        release_date: filmData.release_date ? filmData.release_date : filmData.first_air_date,
                        media_type: filmData.media_type,
                        revenue: filmData.revenue ? filmData.revenue : null,
                        runtime: filmData.runtime ? filmData.runtime : filmData.episode_run_time,
                        budget: filmData.budget ? filmData.budget : null,
                        genres: filmData.genres,
                        credits: filmData.credits,
                    } : {}

                    await pushFile(
                        JSON.stringify({...programData, ...infoToAppend}, null, 2),
                        station.id,
                        station.type
                    );
                    console.log(`✓ Uploaded ${station.type} data for ${station.id} from ${source.name}`);
                    totalCollected++;
                } else if (programData && Array.isArray(programData)) {
                    await programData.forEach( (stationData) => {
                        pushFile(
                            JSON.stringify(stationData, null, 2),
                            stationData.channel,
                            station.type
                        );
                        console.log(`✓ Uploaded ${station.type} data for ${stationData.channel} from ${source.name}`);
                    } )
                } else if(!programData) {
                    console.log(`⚠ No data received for ${station.id}`);
                }
            } catch (error) {
                console.error(`✗ Error processing ${station.id} from ${source.name}:`, error.message);
            }
        }
    }

    console.log(`\n[${new Date().toISOString()}] TV data collection completed. Total: ${totalCollected} items`);
}

// Schedule cron job to run every 5 minutes for all TV sources
console.log('Starting multi-source TV data collection service...');
console.log('Schedule: every 5 minutes (*/5 * * * *)');

cron.schedule('*/5 * * * *', () => {
    collectAndStoreTVData().then(r => console.log(r));
});

// Run once immediately when starting
console.log('Running initial TV data collection...');
collectAndStoreTVData().then(r => console.log(r));

console.log('Multi-source TV cron job scheduled successfully!');