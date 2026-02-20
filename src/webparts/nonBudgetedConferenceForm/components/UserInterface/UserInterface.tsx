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
import { RequestSubForm } from './RequestSubForm';
import { MyRequestsQueueList } from './MyRequestsQueueList';
import { IConferenceRequest } from '../../../../models/IConferenceRequest';

// Trigger IDE cache refresh

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
    const [draftToEdit, setDraftToEdit] = useState<IConferenceRequest | undefined>(undefined);

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
        const newValue = data.value as string;
        if (newValue === 'form' && activeTab !== 'form') {
            // Clear draft when manually switching to the form tab
            setDraftToEdit(undefined);
        }
        setActiveTab(newValue);
    };

    const handleEditDraft = (req: IConferenceRequest) => {
        setDraftToEdit(req);
        setActiveTab('form');
    };

    return (
        <div className={styles.root}>
            <TabList selectedValue={activeTab} onTabSelect={handleTabSelect}>
                <Tab value="form">New Request</Tab>
                <Tab value="queue">My Requests</Tab>
            </TabList>

            {activeTab === 'form' && (
                <RequestSubForm
                    draftData={draftToEdit}
                    onSubmitSuccess={() => {
                        setDraftToEdit(undefined);
                        setActiveTab('queue');
                    }}
                />
            )}

            {activeTab === 'queue' && (
                loading ? (
                    <Spinner label="Loading your requests..." />
                ) : (
                    <MyRequestsQueueList requests={myRequests} onEditDraft={handleEditDraft} />
                )
            )}
        </div>
    );
};
