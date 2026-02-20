import { useState, useEffect } from 'react';
import { GraphService } from '../services/GraphService';
import { IUser } from '../models/IUser';

export const useManagerCheck = (graphService: GraphService) => {
    const [manager, setManager] = useState<IUser | null>(null);
    const [directReports, setDirectReports] = useState<IUser[]>([]);
    const [loading, setLoading] = useState<boolean>(true);

    useEffect(() => {
        let isMounted = true;

        const fetchGraphData = async () => {
            try {
                const [mgr, reports] = await Promise.all([
                    graphService.getMyManager(),
                    graphService.getMyDirectReports()
                ]);

                if (isMounted) {
                    setManager(mgr);
                    setDirectReports(reports);
                    setLoading(false);
                }
            } catch (error) {
                console.error('Error fetching manager/reports:', error);
                if (isMounted) setLoading(false);
            }
        };

        void fetchGraphData();

        return () => {
            isMounted = false;
        };
    }, [graphService]);

    return { manager, directReports, hasDirectReports: directReports.length > 0, loading };
};
