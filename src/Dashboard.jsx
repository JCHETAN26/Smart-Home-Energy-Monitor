import React, { useState } from 'react';
import { Line, Bar, Pie } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import ChartDataLabels from 'chartjs-plugin-datalabels';
import './Dashboard.css';
import './DashboardCharts.css';

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
  ChartDataLabels
);

// Helper functions for data aggregation
const aggregateConsumptionByTime = (readings, hours = 1) => {
  const consumptionByMinute = {};
  const startTime = new Date(Date.now() - hours * 60 * 60 * 1000);

  readings.forEach(item => {
    const itemTime = new Date(item.timestamp);
    if (itemTime > startTime) {
      const minuteKey = new Date(itemTime.getFullYear(), itemTime.getMonth(), itemTime.getDate(), itemTime.getHours(), itemTime.getMinutes()).toISOString();
      consumptionByMinute[minuteKey] = (consumptionByMinute[minuteKey] || 0) + item.consumption_kwh;
    }
  });

  const labels = Object.keys(consumptionByMinute).sort();
  const values = labels.map(key => consumptionByMinute[key]);

  return { labels, values };
};

const aggregateConsumptionByDevice = (readings, hours = 1) => {
  const consumptionByDevice = {};
  const startTime = new Date(Date.now() - hours * 60 * 60 * 1000);

  readings.forEach(item => {
    const itemTime = new Date(item.timestamp);
    if (itemTime > startTime) {
      consumptionByDevice[item.device_id] = (consumptionByDevice[item.device_id] || 0) + item.consumption_kwh;
    }
  });

  const sortedDevices = Object.keys(consumptionByDevice).sort((a, b) => consumptionByDevice[b] - consumptionByDevice[a]);
  const labels = sortedDevices.slice(0, 5);
  const values = labels.map(device => consumptionByDevice[device]);

  return { labels, values };
};

// Function to generate smart suggestion based on consumption
const generateSmartSuggestion = (consumption) => {
  if (consumption > 5) {
    return 'Consider turning off unused appliances or using energy-saving mode.';
  } else if (consumption >= 2 && consumption <= 5) {
    return 'Moderate usage. Try using appliances during off-peak hours to save energy.';
  } else {
    return 'Great! Your energy usage is low. Keep it up!';
  }
};

const Dashboard = ({ apiData }) => {
  const {
    recentReadings = [],
    dailySummaries = [],
    anomalies = [],
    consumptionByDevice = {}
  } = apiData;

  const [cardView, setCardView] = useState('date');
  const [consumptionFilter, setConsumptionFilter] = useState(null);
  const [timeFrame, setTimeFrame] = useState(1); // 1 hour default
  
  const today = new Date().toISOString().slice(0, 10);
  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  const todaySummary = dailySummaries.find(s => s.date === today) || {};
  const yesterdaySummary = dailySummaries.find(s => s.date === yesterday) || {};

  const consumptionTrendData = aggregateConsumptionByTime(recentReadings, timeFrame);
  const deviceConsumptionBarData = aggregateConsumptionByDevice(recentReadings);

  const lineChartData = {
    labels: consumptionTrendData.labels,
    datasets: [
      {
        label: 'Energy Consumption',
        data: consumptionTrendData.values,
        borderColor: 'rgba(75, 192, 192, 1)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        tension: 0.4,
        fill: true,
        pointRadius: 3,
      },
    ],
  };

  const barChartData = {
    labels: deviceConsumptionBarData.labels,
    datasets: [
      {
        label: 'Consumption',
        data: deviceConsumptionBarData.values,
        backgroundColor: 'rgba(54, 162, 235, 0.7)',
      },
    ],
  };

  const pieChartData = {
    labels: Object.keys(consumptionByDevice),
    datasets: [{
      data: Object.values(consumptionByDevice),
      backgroundColor: [
        'rgba(0, 123, 255, 0.8)',
        'rgba(40, 167, 69, 0.8)',
        'rgba(255, 193, 7, 0.8)',
        'rgba(23, 162, 184, 0.8)',
        'rgba(108, 117, 125, 0.8)',
        'rgba(201, 203, 207, 0.8)',
      ],
      borderColor: '#fff',
      borderWidth: 2,
    }],
  };

  // -- CHART OPTIONS --
  const commonChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    devicePixelRatio: window.devicePixelRatio || 1,
    scales: {
      y: {
        beginAtZero: true,
        title: { display: true, text: 'Consumption (kWh)', color: 'var(--secondary-color)', font: { size: 14 } },
        grid: { color: 'var(--border-color)', drawBorder: false },
        ticks: { color: 'var(--light-text-color)' },
      },
      x: {
        grid: { color: 'var(--border-color)', drawBorder: false },
        ticks: { maxTicksLimit: 12, autoSkip: true, color: 'var(--light-text-color)' },
      },
    },
    plugins: {
      tooltip: {
        backgroundColor: 'rgba(0,0,0,0.7)',
        titleColor: '#fff',
        bodyColor: '#fff',
        borderColor: 'var(--primary-color)',
        borderWidth: 1,
        cornerRadius: 4,
      },
      legend: { display: false },
      datalabels: { display: false },
    },
    animation: { duration: 500 },
  };

  const lineChartOptions = {
    ...commonChartOptions,
    plugins: {
      ...commonChartOptions.plugins,
      legend: { display: true, position: 'top', labels: { color: 'var(--text-color)' } },
    },
  };

  const barChartOptions = {
    ...commonChartOptions,
    plugins: {
      ...commonChartOptions.plugins,
      datalabels: {
        anchor: 'end',
        align: 'top',
        formatter: (value) => value.toFixed(2) + ' kWh',
        color: 'var(--text-color)',
        font: { weight: 'bold' },
      },
    },
  };

  const pieChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    devicePixelRatio: window.devicePixelRatio || 1,
    plugins: {
      legend: { display: true, position: 'right', labels: { color: 'var(--text-color)' } },
      datalabels: {
        formatter: (value, context) => {
          const total = context.dataset.data.reduce((sum, val) => sum + val, 0);
          const percentage = (value / total * 100).toFixed(1);
          return percentage > 5 ? `${percentage}%` : '';
        },
        color: '#fff',
        font: { weight: 'bold' },
      },
    },
  };

  const getFilteredReadings = () => {
    let filtered = [...recentReadings];

    if (cardView === 'anomalies') {
      filtered = filtered.filter(item => item.anomaly_detected);
    } else if (cardView === 'consumption') {
      filtered.sort((a, b) => b.consumption_kwh - a.consumption_kwh);
    } else {
      filtered.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    }

    if (consumptionFilter) {
      if (consumptionFilter === 'high') {
        filtered = filtered.filter(item => item.consumption_kwh > 5.0);
      } else if (consumptionFilter === 'medium') {
        filtered = filtered.filter(item => item.consumption_kwh >= 2.0 && item.consumption_kwh <= 5.0);
      } else if (consumptionFilter === 'low') {
        filtered = filtered.filter(item => item.consumption_kwh < 2.0);
      }
    }

    return filtered;
  };

  const filteredReadings = getFilteredReadings();

  return (
    <div className="dashboard-container">
      <header className="dashboard-header">
        <h1>Smart Home Energy Monitor</h1>
        <p className="status-message">Data last updated: {new Date().toLocaleTimeString()} (Auto-refresh)</p>
      </header>

      <section className="overview-metrics-grid">
        <div className="metric-card">
          <h3>Today vs Yesterday</h3>
          <p className="metric-value">
            {todaySummary.total_consumption_kwh && yesterdaySummary.total_consumption_kwh ?
              (todaySummary.total_consumption_kwh / yesterdaySummary.total_consumption_kwh * 100).toFixed(0) + '% of yesterday' : '--'}
          </p>
        </div>
        <div className="metric-card">
          <h3>Today's Total KWh</h3>
          <p className="metric-value">
            {todaySummary.total_consumption_kwh?.toFixed(2) || '--'} kWh
          </p>
        </div>
        <div className="metric-card">
          <h3>Anomalies Detected</h3>
          <p className="metric-value">
            {anomalies.length > 0 ? anomalies.length + ' (recent)' : 'None (recent)'}
          </p>
        </div>
        <div className="metric-card">
          <h3>Today's Est. Cost</h3>
          <p className="metric-value">
            ${todaySummary.total_cost_usd?.toFixed(2) || '--'}
          </p>
        </div>
      </section>

      <section className="main-charts-grid">
        <div className="chart-panel">
          <div className="chart-panel-header">
            <h2>Consumption Trends</h2>
            <div className="time-frame-controls">
                <button onClick={() => setTimeFrame(1)} className={timeFrame === 1 ? 'active' : ''}>1 hr</button>
                <button onClick={() => setTimeFrame(3)} className={timeFrame === 3 ? 'active' : ''}>3 hrs</button>
                <button onClick={() => setTimeFrame(6)} className={timeFrame === 6 ? 'active' : ''}>6 hrs</button>
                <button onClick={() => setTimeFrame(12)} className={timeFrame === 12 ? 'active' : ''}>12 hrs</button>
                <button onClick={() => setTimeFrame(24)} className={timeFrame === 24 ? 'active' : ''}>Yesterday</button>
            </div>
          </div>
          <div className="chart-container">
            <Line data={lineChartData} options={lineChartOptions} />
          </div>
        </div>
        <div className="chart-panel">
          <h2>Consumption Breakdown (Last 24 Hours)</h2>
          <div className="chart-container">
            <Pie data={pieChartData} options={pieChartOptions} />
          </div>
        </div>
      </section>

      <section className="insights-grid">
        <div className="panel">
          <h2>Detected Anomalies</h2>
          <ul id="anomalyList" className="anomaly-list">
            {anomalies.slice(0, 5).map((anomaly, index) => (
              <li key={index}>
                <strong>[{new Date(anomaly.timestamp).toLocaleString()}]</strong> {anomaly.device_id}: {anomaly.anomaly_message}
              </li>
            ))}
            {anomalies.length === 0 && (
              <li>No anomalies detected recently. Keep up the good work!</li>
            )}
          </ul>
        </div>
        <div className="panel">
          <h2>Top Devices by Consumption (Last 24 Hours)</h2>
          <div className="chart-container">
            <Bar data={barChartData} options={barChartOptions} />
          </div>
        </div>
        <div className="panel full-width">
          <h2>Daily Consumption Summary (Last 7 Days)</h2>
          <div className="daily-summary-table-container">
            <table>
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Total kWh</th>
                  <th>Est. Cost</th>
                  <th>Peak Device</th>
                  <th>Peak kWh</th>
                </tr>
              </thead>
              <tbody>
                {dailySummaries.sort((a, b) => new Date(b.date) - new Date(a.date)).map((summary, index) => (
                  <tr key={index}>
                    <td>{summary.date}</td>
                    <td>{summary.total_consumption_kwh.toFixed(3)}</td>
                    <td>${summary.total_cost_usd.toFixed(2)}</td>
                    <td>{summary.peak_device_daily || 'N/A'}</td>
                    <td>{summary.peak_device_consumption_daily?.toFixed(3) || 'N/A'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <section className="recent-readings-panel">
        <div className="readings-panel-header">
          <h2>Recent Device Readings</h2>
          <div className="view-controls">
            <span>View by:</span>
            <button onClick={() => setCardView('date')} className={cardView === 'date' ? 'active' : ''}>Date</button>
            <button onClick={() => setCardView('consumption')} className={cardView === 'consumption' ? 'active' : ''}>Consumption</button>
            <button onClick={() => setCardView('anomalies')} className={cardView === 'anomalies' ? 'active' : ''}>Anomalies</button>
          </div>
          {cardView === 'consumption' && (
            <div className="consumption-sub-controls">
              <span>Consumption:</span>
              <button onClick={() => setConsumptionFilter('high')} className={consumptionFilter === 'high' ? 'active' : ''}>High</button>
              <button onClick={() => setConsumptionFilter('medium')} className={consumptionFilter === 'medium' ? 'active' : ''}>Medium</button>
              <button onClick={() => setConsumptionFilter('low')} className={consumptionFilter === 'low' ? 'active' : ''}>Low</button>
            </div>
          )}
        </div>
        <div className="recent-readings-grid">
          {filteredReadings.slice(0, 20).map((item, index) => (
            <div key={index} className="data-card">
              <p><strong>Device:</strong> {item.device_id}</p>
              <p><strong>Location:</strong> {item.location}</p>
              <p><strong>Consumption:</strong> {item.consumption_kwh.toFixed(3)} kWh</p>
              <p><strong>Status:</strong> {item.status}</p>
              <p><strong>Time:</strong> {new Date(item.timestamp).toLocaleTimeString()}</p>
              <p><strong>Anomaly:</strong> {item.anomaly_detected ? <span style={{ color: 'red', fontWeight: 'bold' }}>Yes!</span> : 'No'}</p>
              <p><strong>Suggestion:</strong> {generateSmartSuggestion(item.consumption_kwh)}</p>
            </div>
          ))}
          {filteredReadings.length === 0 && (
            <p className="no-readings-message">No readings match the selected view.</p>
          )}
        </div>
      </section>
    </div>
  );
};

export default Dashboard;
