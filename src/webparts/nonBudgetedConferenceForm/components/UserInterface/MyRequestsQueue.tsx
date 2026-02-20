import * as React from 'react';
import {
    makeStyles,
    Card,
    CardHeader,
    Text,
    tokens,
    shorthands
} from '@fluentui/react-components';
import { IConferenceRequest } from '../../../../models/IConferenceRequest';
import { StatusBadge } from '../Shared/StatusBadge';

const useStyles = makeStyles({
    queueList: {
        display: 'flex',
        flexDirection: 'column',
        gap: '12px'
    },
    card: {
        ...shorthands.margin('0px'),
        width: '100%',
        maxWidth: '100%'
    },
    row: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '8px'
    },
    denialReason: {
        color: tokens.colorPaletteRedForeground1,
        marginTop: '8px',
        paddingTop: '8px',
        borderTop: `1px solid ${tokens.colorNeutralStroke1}`
    }
});

interface IMyRequestsQueueProps {
    requests: IConferenceRequest[];
}

export const MyRequestsQueue: React.FC<IMyRequestsQueueProps> = ({ requests }) => {
    const styles = useStyles();

    if (requests.length === 0) {
        return (
            <Text>You haven't submitted any requests yet.</Text>
        );
    }

    return (
        <div className={styles.queueList}>
            {requests.map(req => (
                <Card key={req.Id} className={styles.card}>
                    <CardHeader
                        header={
                            <div className={styles.row}>
                                <Text weight="semibold" size={400}>{req.EventName}</Text>
                                <StatusBadge status={req.Status} />
                            </div>
                        }
                        description={
                            <Text size={300}>
                                {new Date(req.EventStartDate || '').toLocaleDateString()} - {new Date(req.EventEndDate || '').toLocaleDateString()}
                            </Text>
                        }
                    />

                    <div style={{ padding: '0 12px 12px 12px' }}>
                        <Text block><strong>Location:</strong> {req.EventLocation}</Text>
                        <Text block><strong>Total Est. Budget:</strong> ${req.TotalEstimatedBudget?.toFixed(2) || '0.00'}</Text>

                        {req.Status === 'Denied' && req.ManagerDenialReason && (
                            <div className={styles.denialReason}>
                                <Text><strong>Reason for Denial (Manager):</strong> {req.ManagerDenialReason}</Text>
                            </div>
                        )}
                        {req.Status === 'Denied' && req.OrgDevDenialReason && (
                            <div className={styles.denialReason}>
                                <Text><strong>Reason for Denial (Org Dev):</strong> {req.OrgDevDenialReason}</Text>
                            </div>
                        )}
                        {req.Status === 'Denied' && req.AccountingDenialReason && (
                            <div className={styles.denialReason}>
                                <Text><strong>Reason for Denial (Accounting):</strong> {req.AccountingDenialReason}</Text>
                            </div>
                        )}

                        {req.GLCode && (
                            <Text block style={{ marginTop: 8, color: tokens.colorPaletteGreenForeground1 }}>
                                <strong>Assigned GL Code:</strong> {req.GLCode}
                            </Text>
                        )}
                    </div>
                </Card>
            ))}
        </div>
    );
};
