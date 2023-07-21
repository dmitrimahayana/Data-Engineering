import React, { useState, useEffect } from 'react';
import Button from '@mui/material/Button';
import TextField from '@mui/material/TextField';
import Grid from '@mui/material/Grid';
import Paper from '@mui/material/Paper';
import { styled } from '@mui/material/styles';
import Box from '@mui/material/Box';
import { DataGrid } from '@mui/x-data-grid';

function App() {
    const [posts, setPosts] = useState([]);
    const [keyword, setKeyword] = useState('');
    var ticker = 'BBNI';

    // GET with fetch API
    useEffect(() => {
        const fetchPost = async () => {
            const response = await fetch(
                'http://localhost:9090/ticker-list-last-stock'
            );
            const data = await response.json();
            console.log(data);
            setPosts(data);
        };
        fetchPost();
    }, []);

    const addPosts = async (keyword) => {
        ticker = keyword.toUpperCase();
        console.log("search ticker: " + ticker)
        const fetchPost = async () => {
            const response = await fetch(
                'http://localhost:9090/ticker/' + ticker
            );
            const data = await response.json();
            console.log(data);
            setPosts(data);
        };
        fetchPost();
    };

    const handleSubmit = (e) => {
        e.preventDefault();
        addPosts(keyword);
    };

    const Item = styled(Paper)(({ theme }) => ({
        padding: theme.spacing(1),
        // color: theme.palette.text.secondary,
    }));

    const columns = [
        { field: 'id', headerName: 'ID', width: 150 },
        {
            field: 'ticker',
            headerName: 'Ticker',
            width: 100,
            editable: true,
        },
        {
            field: 'date',
            headerName: 'Date',
            width: 100,
            editable: true,
        },
        {
            field: 'volume',
            headerName: 'Volume',
            type: 'number',
            width: 120,
            editable: true,
        },
        {
            field: 'open',
            headerName: 'Open',
            type: 'number',
            width: 100,
            editable: true,
        },
        {
            field: 'close',
            headerName: 'Close',
            type: 'number',
            width: 100,
            editable: true,
        },
    ];

    // const rows = [
    //     {posts.map((post) => {
    //         return (
    //             post.ticker + "," + post.date
    //         )}}
    //     //     return(post.ticker);
    //     // });
    // ];
    // { id: post.ticker + "-" + post.date, Ticker: post.ticker, Date: post.date, Open: post.open, Volume: post.volume, Close: post.close }

    return (
        <Box sx={{ flexGrow: 1 }}>
            <Box
                sx={{
                    display: 'flex',
                    justifyContent: 'flex-start',
                    p: 1,
                    m: 1,
                    bgcolor: 'background.paper',
                    borderRadius: 1,
                }}>
                <form onSubmit={handleSubmit}>
                    <Box component="span" sx={{ p: 0 }}>
                        <TextField size="small" id="outlined-basic" label="Ticker Name" variant="outlined" type="text" className="form-control" id="" value={keyword}
                            onChange={(e) => setKeyword(e.target.value)}
                        />
                    </Box>
                    <Box component="span" sx={{ p: 1 }}>
                        <Button size="medium" variant="contained" type="submit">Search History Ticker</Button>
                    </Box>
                </form>
            </Box>
            <Box sx={{ height: 480, width: '100%' }}>
                <DataGrid
                    rows={posts}
                    columns={columns}
                    initialState={{
                        pagination: {
                            paginationModel: {
                                pageSize: 7,
                            },
                        },
                    }}
                    pageSizeOptions={[5]}
                    checkboxSelection
                    disableRowSelectionOnClick
                />
            </Box>
            {/* <article>
                <ul>
                    {posts.map((post) => {
                        return (
                            <li key={post.ticker + "-" + post.date}>
                                <b><a className="post-ticker">{"Ticker: " + post.ticker}</a><br />
                                    <a className="post-date">{"Date: " + post.date}</a><br /></b>
                                <a className="post-open">{"Open: " + post.open}</a><br />
                                <a className="post-volume">{"Volume: " + post.volume}</a><br />
                                <b><a className="post-close">{"Close: " + post.close}</a></b><br /><br />
                            </li>
                        );
                    })}
                </ul>
            </article> */}
        </Box >
    );
};

export default App;
