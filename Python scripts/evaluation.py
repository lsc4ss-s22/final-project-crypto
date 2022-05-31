from sklearn import metrics

def dynamic_evaluation(real, predicted_all, predicted_part, eva, threshold=0.5, type_='binary', set_='all'):
    if set_ == 'all':
        acc_all = []
        acc_part = []
        if type_ == 'binary':
            for all_, part_, real_ in zip(predicted_all, predicted_part, real):
                acc_all.append(eva([i[0] for i in real_], [1 if i >= threshold else 0 for i in all_]))
                acc_part.append(eva([i[0] for i in real_], [1 if i >= threshold else 0 for i in part_]))
        else:
            for all_, part_, real_ in zip(predicted_all, predicted_part, real):
                acc_all.append(eva(real_, all_))
                acc_part.append(eva(real_, part_))
    else:
        if type_ == 'binary':
            acc_all = eva(real, predicted_all)
            acc_part = eva(real, predicted_part)
    return acc_all, acc_part

def false_negative_rate(real, predict):
    tn, fp, fn, tp = metrics.confusion_matrix(real, predict).ravel()
    false_negative_rate = fn / (tp + fn)
    return false_negative_rate

def false_positive_rate(real, predict):
    tn, fp, fn, tp = metrics.confusion_matrix(real, predict).ravel()
    false_positive_rate = fp / (fp + tn)
    return false_positive_rate
    