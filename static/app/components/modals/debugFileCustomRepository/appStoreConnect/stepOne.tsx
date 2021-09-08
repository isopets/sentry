import {Fragment} from 'react';

import Alert from 'app/components/alert';
import ExternalLink from 'app/components/links/externalLink';
import {t, tct} from 'app/locale';
import Input from 'app/views/settings/components/forms/controls/input';
import Textarea from 'app/views/settings/components/forms/controls/textarea';
import Field from 'app/views/settings/components/forms/field';

import {StepOneData} from './types';

type Props = {
  stepOneData: StepOneData;
  onSetStepOneData: (stepOneData: StepOneData) => void;
};

function StepOne({stepOneData, onSetStepOneData}: Props) {
  return (
    <Fragment>
      <Alert type="info">
        {tct(
          'Please enter the [docLink:App Store Connect API Key] details. The key needs to have the "Developer" role for Sentry to discover the app builds.',
          {
            docLink: (
              <ExternalLink href="https://developer.apple.com/documentation/appstoreconnectapi/creating_api_keys_for_app_store_connect_api" />
            ),
          }
        )}
      </Alert>
      <Field label={t('Issuer')} inline={false} flexibleControlStateSize stacked required>
        <Input
          type="text"
          name="issuer"
          placeholder={t('Issuer')}
          value={stepOneData.issuer}
          onChange={e =>
            onSetStepOneData({
              ...stepOneData,
              issuer: e.target.value,
            })
          }
        />
      </Field>
      <Field label={t('Key ID')} inline={false} flexibleControlStateSize stacked required>
        <Input
          type="text"
          name="keyId"
          placeholder={t('Key Id')}
          value={stepOneData.keyId}
          onChange={e =>
            onSetStepOneData({
              ...stepOneData,
              keyId: e.target.value,
            })
          }
        />
      </Field>
      <Field
        label={t('Private Key')}
        inline={false}
        flexibleControlStateSize
        stacked
        required
      >
        <Textarea
          name="privateKey"
          value={stepOneData.privateKey}
          rows={5}
          autosize
          placeholder={
            stepOneData.privateKey === undefined
              ? t('(Private Key unchanged)')
              : '-----BEGIN PRIVATE KEY-----\n[PRIVATE-KEY]\n-----END PRIVATE KEY-----'
          }
          onChange={e =>
            onSetStepOneData({
              ...stepOneData,
              privateKey: e.target.value,
            })
          }
        />
      </Field>
    </Fragment>
  );
}

export default StepOne;
