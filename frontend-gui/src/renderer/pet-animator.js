// 桌宠动画控制器

export const PET_STATES = {
  IDLE: "idle",
  THINKING: "thinking",
  WORKING: "working",
  SUCCESS: "success",
  ERROR: "error"
};

/**
 * 桌宠动画控制器
 */
export class PetAnimator {
  /**
   * @param {HTMLElement} petEl - 桌宠元素
   */
  constructor(petEl) {
    this.element = petEl;
    this.currentState = PET_STATES.IDLE;
  }

  /**
   * 设置桌宠状态
   * @param {string} state - 状态名称
   */
  setState(state) {
    if (!this.element || !PET_STATES[state.toUpperCase()]) return;

    // 移除所有状态类
    Object.values(PET_STATES).forEach(s => {
      this.element.classList.remove(`pet-state-${s}`);
    });

    // 添加新状态类
    this.element.classList.add(`pet-state-${state}`);
    this.currentState = state;
  }

  /**
   * 获取当前状态
   * @returns {string}
   */
  getState() {
    return this.currentState;
  }

  /**
   * 播放一次性动画
   * @param {string} animationName - 动画名称（success/error）
   * @param {Function} callback - 动画结束回调
   */
  playAnimation(animationName, callback) {
    if (!this.element) return;

    // 保存原状态
    const originalState = this.currentState;

    // 播放动画状态
    this.setState(animationName);

    // 动画结束后恢复
    setTimeout(() => {
      this.setState(originalState);
      if (callback) callback();
    }, 600);
  }

  /**
   * 重置为空闲状态
   */
  reset() {
    this.setState(PET_STATES.IDLE);
  }
}
