import { useState, useEffect } from 'react';
import { fetchSources, fetchSourceData } from '../api/sources';
import { MapSource } from '../types/sources';

const SOURCE_COLORS = {
    agricultural_fields: '#4CAF50',
    wetlands: '#2196F3'
};

export function useMapData() {
    const [sources, setSources] = useState<MapSource[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const loadData = async () => {
            try {
                const availableSources = await fetchSources();
                const sourceData = await Promise.all(
                    Object.keys(availableSources).map(async (sourceId) => {
                        const data = await fetchSourceData(sourceId);
                        return {
                            id: sourceId,
                            data,
                            color: SOURCE_COLORS[sourceId as keyof typeof SOURCE_COLORS],
                            opacity: 0.6
                        };
                    })
                );
                setSources(sourceData);
            } catch (e) {
                setError(e instanceof Error ? e.message : 'Unknown error');
            } finally {
                setLoading(false);
            }
        };

        loadData();
    }, []);

    return { sources, loading, error };
}
