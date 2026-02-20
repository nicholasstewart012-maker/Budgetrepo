import * as React from 'react';
import { useState, useEffect } from 'react';
import {
    makeStyles,
    TabList,
    Tab,
    SelectTabData,
    SelectTabEvent,
    Spinner
} from '@fluentui/react-components';
import { useAppContext } from '../../../../context/AppContext';
import { RequestForm } from './RequestForm';
import { MyRequestsQueue } from './MyRequestsQueue';
import { IConferenceRequest } from '../../../../models/IConferenceRequest';

const useStyles = makeStyles({
    root: {
        display: 'flex',
        flexDirection: 'column',
        gap: '20px'
    }
});

export const UserInterface: React.FC = () => {
    const styles = useStyles();
    const { spService, currentUser } = useAppContext();

    const [activeTab, setActiveTab] = useState<string>('form');
    const [myRequests, setMyRequests] = useState<IConferenceRequest[]>([]);
    const [loading, setLoading] = useState(false);

    const loadMyRequests = async () => {
        setLoading(true);
        try {
            const filter = `SubmitterEmail eq '${currentUser.email}'`;
            const requests = await spService.getRequests(filter);
            setMyRequests(requests);
        } catch (error) {
            console.error('Error loading my requests:', error);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        if (activeTab === 'queue') {
            void loadMyRequests();
        }
    }, [activeTab]);

    const handleTabSelect = (event: SelectTabEvent, data: SelectTabData) => {
        setActiveTab(data.value as string);
    };

    return (
        <div className={styles.root}>
            <TabList selectedValue={activeTab} onTabSelect={handleTabSelect}>
                <Tab value="form">New Request</Tab>
                <Tab value="queue">My Requests</Tab>
            </TabList>

            {activeTab === 'form' && (
                <RequestForm onSubmitSuccess={() => setActiveTab('queue')} />
            )}

            {activeTab === 'queue' && (
                loading ? (
                    <Spinner label="Loading your requests..." />
                ) : (
                    <MyRequestsQueue requests={myRequests} />
                )
            )}
        </div>
    );
};
