import { useMemo } from 'react';
import { WebPartContext } from '@microsoft/sp-webpart-base';
import { IUser } from '../models/IUser';

export const useCurrentUser = (context: WebPartContext): IUser => {
    return useMemo(() => {
        return {
            displayName: context.pageContext.user.displayName,
            email: context.pageContext.user.email,
        };
    }, [context]);
};
