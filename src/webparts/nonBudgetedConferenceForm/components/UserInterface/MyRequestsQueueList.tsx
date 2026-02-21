import * as React from 'react';
import {
    makeStyles,
    Card,
    CardHeader,
    Text,
    Button,
    tokens,
    shorthands
} from '@fluentui/react-components';
import { IConferenceRequest } from '../../../../models/IConferenceRequest';
import { StatusBadge } from '../Shared/StatusBadge';
import { RequestDetailsModal } from '../Shared/RequestDetailsModal';

const useStyles = makeStyles({
    queueList: {
        display: 'flex',
        flexDirection: 'column',
        gap: '16px'
    },
    card: {
        ...shorthands.margin('0px'),
        width: '100%',
        maxWidth: '100%',
        boxShadow: tokens.shadow2,
        border: `1px solid ${tokens.colorNeutralStroke1}`
    },
    row: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        paddingBottom: '8px',
        borderBottom: `1px solid ${tokens.colorNeutralStroke2}`
    },
    metaRow: {
        display: 'flex',
        gap: '24px',
        flexWrap: 'wrap',
        paddingTop: '12px'
    },
    metricBlock: {
        display: 'flex',
        flexDirection: 'column',
        gap: '2px'
    },
    metricLabel: {
        fontSize: '11px',
        color: tokens.colorNeutralForeground3,
        textTransform: 'uppercase',
        letterSpacing: '0.5px',
        fontWeight: 'semibold'
    },
    metricValue: {
        fontSize: '14px',
        color: tokens.colorNeutralForeground1,
        fontWeight: 'semibold'
    },
    denialReason: {
        backgroundColor: tokens.colorPaletteRedBackground1,
        ...shorthands.padding('12px'),
        ...shorthands.borderRadius('6px'),
        marginTop: '16px',
        borderLeft: `4px solid ${tokens.colorPaletteRedForeground1}`
    },
    actions: {
        display: 'flex',
        justifyContent: 'flex-end',
        gap: '8px',
        marginTop: '16px',
        paddingTop: '16px',
        borderTop: `1px dashed ${tokens.colorNeutralStroke2}`
    }
});

interface IMyRequestsQueueListProps {
    requests: IConferenceRequest[];
    onEditDraft: (request: IConferenceRequest) => void;
}

export const MyRequestsQueueList: React.FC<IMyRequestsQueueListProps> = ({ requests, onEditDraft }) => {
    const styles = useStyles();
    const [selectedRequest, setSelectedRequest] = React.useState<IConferenceRequest | undefined>(undefined);

    if (requests.length === 0) {
        return (
            <div style={{ textAlign: 'center', padding: '40px', color: tokens.colorNeutralForeground3 }}>
                <Text size={400}>You haven't submitted any requests yet.</Text>
            </div>
        );
    }

    return (
        <div className={styles.queueList}>
            {requests.map(req => (
                <Card key={req.Id} className={styles.card}>
                    <div style={{ padding: '16px' }}>
                        <div className={styles.row}>
                            <Text weight="bold" size={400}>{req.EventName}</Text>
                            <StatusBadge status={req.Status} />
                        </div>

                        <div className={styles.metaRow}>
                            <div className={styles.metricBlock}>
                                <Text className={styles.metricLabel}>DATES</Text>
                                <Text className={styles.metricValue}>
                                    {req.EventStartDate ? new Date(req.EventStartDate).toLocaleDateString() : 'TBD'} -
                                    {req.EventEndDate ? new Date(req.EventEndDate).toLocaleDateString() : 'TBD'}
                                </Text>
                            </div>
                            <div className={styles.metricBlock}>
                                <Text className={styles.metricLabel}>LOCATION</Text>
                                <Text className={styles.metricValue}>{req.EventLocation}</Text>
                            </div>
                            <div className={styles.metricBlock}>
                                <Text className={styles.metricLabel}>TOTAL BUDGET</Text>
                                <Text className={styles.metricValue}>${req.TotalEstimatedBudget?.toFixed(2) || '0.00'}</Text>
                            </div>
                            {req.GLCode && (
                                <div className={styles.metricBlock}>
                                    <Text className={styles.metricLabel}>GL CODE</Text>
                                    <Text className={styles.metricValue} style={{ color: tokens.colorPaletteGreenForeground1 }}>{req.GLCode}</Text>
                                </div>
                            )}
                        </div>

                        {req.Status === 'Denied' && (
                            <div className={styles.denialReason}>
                                <Text weight="semibold" style={{ color: tokens.colorPaletteRedForeground1 }}>Reason for Denial:</Text>
                                <Text block style={{ color: tokens.colorPaletteRedForeground3, marginTop: '4px' }}>
                                    {req.ManagerDenialReason || req.OrgDevDenialReason || req.AccountingDenialReason || 'No reason provided.'}
                                </Text>
                            </div>
                        )}

                        <div className={styles.actions}>
                            <Button appearance="subtle" onClick={() => setSelectedRequest(req)}>View Details</Button>
                            {req.Status === 'Draft' && (
                                <Button appearance="primary" onClick={() => onEditDraft(req)}>Edit Draft</Button>
                            )}
                        </div>
                    </div>
                </Card>
            ))}

            <RequestDetailsModal
                isOpen={!!selectedRequest}
                request={selectedRequest}
                onClose={() => setSelectedRequest(undefined)}
            />
        </div>
    );
};
