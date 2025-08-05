import { useState, useEffect } from 'react';
import Dashboard from './Dashboard';
import './App.css';

// REPLACE THIS WITH YOUR ACTUAL API GATEWAY INVOKE URL
const API_URL = 'https://xfidwiodxj.execute-api.us-east-1.amazonaws.com/prod/data'; 

function App() {
  const [apiData, setApiData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    async function fetchData() {
      try {
        const response = await fetch(API_URL);
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        setApiData(data);
      } catch (e) {
        setError(e.message);
        console.error('Error fetching data:', e);
      } finally {
        setIsLoading(false);
      }
    }

    // Fetch data immediately on mount
    fetchData();

    // Set up an interval to refresh data every 10 seconds
    const interval = setInterval(fetchData, 10000);

    // Clean up the interval on unmount
    return () => clearInterval(interval);
  }, []);

  if (isLoading) {
    return (
      <div className="loading-container">
        <p>Loading dashboard...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="error-container">
        <p>Error: {error}</p>
        <p>Could not fetch data from the API. Please check the network connection and the API endpoint.</p>
      </div>
    );
  }
  
  if (!apiData || Object.keys(apiData).length === 0) {
    return (
      <div className="no-data-container">
        <p>No data available yet. Please wait for the pipeline to produce data.</p>
      </div>
    );
  }
  
  return <Dashboard apiData={apiData} />;
}

export default App;