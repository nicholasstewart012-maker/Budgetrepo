import * as React from 'react';
import { useState, useEffect } from 'react';
import {
    makeStyles,
    TabList,
    Tab,
    SelectTabData,
    SelectTabEvent,
    Spinner,
    shorthands,
    tokens
} from '@fluentui/react-components';
import { useAppContext } from '../../../../context/AppContext';
import { RequestSubForm } from './RequestSubForm';
import { MyRequestsQueueList } from './MyRequestsQueueList';
import { IConferenceRequest } from '../../../../models/IConferenceRequest';

const useStyles = makeStyles({
    root: {
        display: 'flex',
        flexDirection: 'column',
        gap: '24px'
    },
    tabCard: {
        backgroundColor: tokens.colorNeutralBackground1,
        ...shorthands.padding('8px', '16px'),
        ...shorthands.borderRadius('8px'),
        boxShadow: tokens.shadow2,
        border: `1px solid ${tokens.colorNeutralStroke1}`
    },
    contentArea: {
        display: 'flex',
        flexDirection: 'column',
        gap: '16px'
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
            <div className={styles.tabCard}>
                <TabList selectedValue={activeTab} onTabSelect={handleTabSelect}>
                    <Tab value="form">New Request</Tab>
                    <Tab value="queue">My Requests</Tab>
                </TabList>
            </div>

            <div className={styles.contentArea}>
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
                        <div style={{ display: 'flex', justifyContent: 'center', padding: '40px' }}>
                            <Spinner label="Loading your requests..." size="large" />
                        </div>
                    ) : (
                        <MyRequestsQueueList requests={myRequests} onEditDraft={handleEditDraft} />
                    )
                )}
            </div>
        </div>
    );
};
