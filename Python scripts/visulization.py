import matplotlib.pyplot as plt
from sklearn.metrics import roc_curve, auc, roc_auc_score
import seaborn as sns
import numpy as np
import pandas as pd


def plot_roc_auc(real, p_all, p_part):
    fpr_all, tpr_all, _ = roc_curve(real, p_all)
    roc_auc_all = auc(fpr_all, tpr_all)
    fpr_part, tpr_part, _ = roc_curve(real, p_part)
    roc_auc_part = auc(fpr_part, tpr_part)
    plt.figure()
    plt.plot(fpr_all, tpr_all, color='#B90000', lw=2,
             label="ROC curve for full model (area = %0.2f)" % roc_auc_all)
    plt.plot(fpr_part, tpr_part, color='#00007E', lw=2, 
             label="ROC curve for part model (area = %0.2f)" % roc_auc_part)
    plt.plot([0, 1], [0, 1], color="black", lw=2, linestyle="--")
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("ROC plot")
    plt.legend(loc="lower right")
    plt.show()
    

def hue_regplot(data, x, y, hue, palette=None, markers=['o', 'x'], **kwargs):
    from matplotlib.cm import get_cmap
    regplots = []
    
    levels = data[hue].unique()
    if palette is None:
        default_colors = ['#F3A712', '#1B263B']
        palette = {k: default_colors[i] for i, k in enumerate(levels)}
        
    for key, marker in zip(levels, markers):
        regplots.append(
            sns.regplot(
                x=x,
                y=y,
                data=data[data[hue] == key],
                color=palette[key],
                marker=marker,
                label=key,
                **kwargs
            )
        )
        sns.despine()
    
    return regplots


def results_print(df, group1='without_forex', group2='with_forex'):
    print("-------------------------------------------------------------------------------------------------\n" +
          f"| \t\t\t\t{group1.upper()}\t\t\t{group2.upper()}\t\t\t|\n" +
          "-------------------------------------------------------------------------------------------------\n" +
          f"| mean accuracy:\t\t{round(np.mean(df[df['type'] == 'without_conv']['accuracy']), 4)}\t\t\t\t{round(np.mean(df[df['type'] == 'with_conv']['accuracy']), 4)}\t\t\t\t|\n" +
          f"| mean precision score:\t\t{round(np.mean(df[df['type'] == 'without_conv']['precision_score']), 4)}\t\t\t\t{round(np.mean(df[df['type'] == 'with_conv']['precision_score']), 4)}\t\t\t\t|\n" +
          f"| mean recall:\t\t\t{round(np.mean(df[df['type'] == 'without_conv']['recall_score']), 4)}\t\t\t\t{round(np.mean(df[df['type'] == 'with_conv']['recall_score']), 4)}\t\t\t\t|\n" +
          f"| mean false positive rate:\t{round(np.mean(df[df['type'] == 'without_conv']['false_positive_rate']), 4)}\t\t\t\t{round(np.mean(df[df['type'] == 'with_conv']['false_positive_rate']), 4)}\t\t\t\t|\n" +
          f"| mean false_negative_rate:\t{round(np.mean(df[df['type'] == 'without_conv']['false_negative_rate']), 4)}\t\t\t\t{round(np.mean(df[df['type'] == 'with_conv']['false_negative_rate']), 4)}\t\t\t\t|\n" +
          "-------------------------------------------------------------------------------------------------")