import * as React from 'react';
import { Badge, makeStyles, tokens } from '@fluentui/react-components';
import { RequestStatus } from '../../../../models/IConferenceRequest';

interface IStatusBadgeProps {
    status: RequestStatus;
}

const useStyles = makeStyles({
    statusBadge: {
        padding: '4px 8px',
        borderRadius: '4px',
        display: 'inline-flex',
        alignItems: 'center',
        fontWeight: 600
    },
    draft: {
        backgroundColor: tokens.colorNeutralBackground3,
        color: tokens.colorNeutralForeground3
    },
    pendingManager: {
        backgroundColor: tokens.colorPaletteYellowBackground1,
        color: tokens.colorPaletteYellowForeground1
    },
    pendingOrgDev: {
        backgroundColor: tokens.colorPaletteDarkOrangeBackground1,
        color: tokens.colorPaletteDarkOrangeForeground1
    },
    pendingAccounting: {
        backgroundColor: tokens.colorPaletteDarkOrangeBackground1,
        color: tokens.colorPaletteDarkOrangeForeground1
    },
    approved: {
        backgroundColor: tokens.colorPaletteGreenBackground1,
        color: tokens.colorPaletteGreenForeground1
    },
    denied: {
        backgroundColor: tokens.colorPaletteRedBackground1,
        color: tokens.colorPaletteRedForeground1
    }
});

export const StatusBadge: React.FC<IStatusBadgeProps> = ({ status }) => {
    const styles = useStyles();

    let appearanceClass = styles.draft;
    switch (status) {
        case 'Draft':
            appearanceClass = styles.draft;
            break;
        case 'Pending Manager Approval':
            appearanceClass = styles.pendingManager;
            break;
        case 'Pending Org Dev Approval':
            appearanceClass = styles.pendingOrgDev;
            break;
        case 'Pending Accounting Approval':
            appearanceClass = styles.pendingAccounting;
            break;
        case 'Fully Approved':
            appearanceClass = styles.approved;
            break;
        case 'Denied':
            appearanceClass = styles.denied;
            break;
    }

    return (
        <Badge className={appearanceClass} size="medium">
            {status}
        </Badge>
    );
};
