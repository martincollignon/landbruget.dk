interface DataInfoProps {
    lastUpdated: string;
    nextUpdate: string;
    dataFreshness: string;
}

export const DataInfo: React.FC<DataInfoProps> = ({ 
    lastUpdated, 
    nextUpdate, 
    dataFreshness 
}) => {
    return (
        <div className="data-info">
            <h3>Data Information</h3>
            <p>Agricultural Fields Data:</p>
            <ul>
                <li>Update Frequency: Weekly (Mondays at 2 AM UTC)</li>
                <li>Last Updated: {new Date(lastUpdated).toLocaleString()}</li>
                <li>Next Update: {new Date(nextUpdate).toLocaleString()}</li>
                <li>Data Freshness: {dataFreshness}</li>
            </ul>
        </div>
    );
}; 