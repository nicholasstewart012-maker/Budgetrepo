import * as React from 'react';
import { Badge, makeStyles } from '@fluentui/react-components';
import { RequestStatus } from '../../../../models/IConferenceRequest';
import { getStatusColor, getReadableTextColor, getCategoryColor } from '../../../../ui/designSystem';

interface IStatusBadgeProps {
    status?: RequestStatus;
    category?: string;
}

const useStyles = makeStyles({
    baseBadge: {
        padding: '4px 12px',
        borderRadius: '999px',
        display: 'inline-flex',
        alignItems: 'center',
        fontWeight: 600,
        fontSize: '12px',
        border: 'none',
        whiteSpace: 'nowrap'
    },
    container: {
        display: 'flex',
        gap: '6px',
        alignItems: 'center',
        flexWrap: 'wrap'
    }
});

export const StatusBadge: React.FC<IStatusBadgeProps> = ({ status, category }) => {
    const styles = useStyles();

    const renderStatusBadge = () => {
        if (!status) return null;
        const bgColor = getStatusColor(status);
        const fgColor = getReadableTextColor(bgColor);

        return (
            <Badge
                className={styles.baseBadge}
                style={{ backgroundColor: bgColor, color: fgColor }}
            >
                {status}
            </Badge>
        );
    };

    const renderCategoryBadge = () => {
        if (!category) return null;
        const catColor = getCategoryColor(category);
        if (!catColor) return null;

        const fgColor = getReadableTextColor(catColor);

        return (
            <Badge
                className={styles.baseBadge}
                style={{ backgroundColor: catColor, color: fgColor }}
            >
                {category}
            </Badge>
        );
    };

    return (
        <div className={styles.container}>
            {renderCategoryBadge()}
            {renderStatusBadge()}
        </div>
    );
};
